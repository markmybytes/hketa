import csv
from datetime import datetime, timedelta
from typing import Generator

import aiohttp
import pytz

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta


@ensure_session
async def routes(*, session: aiohttp.ClientSession) -> dict[str, t.Route]:
    routes_ = {}
    async with session.get('https://opendata.mtr.com.hk/data/mtr_bus_stops.csv') as request:
        for row in csv.reader((await request.text('utf-8')).splitlines()[1:]):
            # column definition:
            #   route, direction, seq, stopID, stopLAT, stopLONG, stopTCName, stopENName
            direction = 'outbound' if row[1] == 'O' else 'inbound'
            routes_.setdefault(row[0], {'outbound': [], 'inbound': []})

            if row[2] == '1.00' or row[2] == '1':
                # orignal
                routes_[row[0]][direction].append({
                    'id': f'{row[0]}_{direction}_1',
                    'description': None,
                    'orig': {
                        'seq': int(row[2].removesuffix('.00')),
                        'name': {'en': row[7], 'tc': row[6]}
                    },
                    'dest': {}
                })
            else:
                # destination
                routes_[row[0]][direction][0]['dest'] = {
                    'seq': int(row[2].removesuffix('.00')),
                    'name': {'en': row[7], 'tc': row[6]}
                }
    return routes_


@ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession) -> Generator[t.Stop, None, None]:
    async with session.get('https://opendata.mtr.com.hk/data/mtr_bus_stops.csv') as request:
        # pylint: disable=line-too-long
        stops_ = [stop for stop in csv.reader((await request.text('utf-8')).splitlines()[1:])
                  if set((stop[0], 'outbound' if stop[1] == 'O' else 'inbound')) == set(route_id.split('_')[:2])]

    if len(stops_) == 0:
        raise KeyError('route not exists')
    return ({
        'id': s[3],
        'seq': int(s[2].removesuffix('.00')),
        'name': {'zh': s[6], 'en': s[7]},
        'location': (s[4], s[5])
    } for s in stops_)


@ ensure_session
async def etas(route_id: str,
               stop_id: str,
               language: t.Language = 'zh',
               *,
               session: aiohttp.ClientSession) -> t.Etas:
    route, *_ = route_id.split('_')

    async with session.post('https://rt.data.gov.hk/v1/transport/mtr/bus/getSchedule',
                            json={'routeName': route, 'language': language}) as request:
        response = await request.json()

    if len(response) == 0:
        return error_eta('api-error')
    if response['routeStatusRemarkTitle'] is not None:
        if response['routeStatusRemarkTitle'] in ('\u505c\u6b62\u670d\u52d9', 'Non-service hours'):
            return error_eta('eos')
        return error_eta(response['routeStatusRemarkTitle'])

    etas_ = []
    timestamp = datetime.strptime(response['routeStatusTime'], '%Y/%m/%d %H:%M')\
        .astimezone(pytz.timezone('Asia/Hong_kong'))

    for stop in (s for s in response['busStop'] if s['busStopId'] == stop_id):
        for eta in stop['bus']:
            time_ref = 'departure' if eta['arrivalTimeInSecond'] == '108000' else 'arrival'
            if any(c.isdigit() for c in eta[f'{time_ref}TimeText']):
                # eta TimeText has numbers (e.g. 3 分鐘/3 Minutes)
                etas_.append({
                    'eta': dt_to_8601(
                        timestamp + timedelta(seconds=int(eta[f'{time_ref}TimeInSecond']))),
                    'is_arriving': False,
                    'is_scheduled': eta['busLocation']['longitude'] == 0,
                    'extras': {
                        'destination': None,
                        'varient': None,
                        'platform': None,
                        'car_length': None
                    },
                    'remark': None
                })
            else:
                etas_.append({
                    'eta': dt_to_8601(timestamp),
                    'is_arriving': True,
                    'is_scheduled': eta['busLocation']['longitude'] == 0,
                    'extras': {
                        'destination': None,
                        'varient': None,
                        'platform': None,
                        'car_length': None
                    },
                    'remark': None,
                })

    return {
        'timestamp': dt_to_8601(timestamp),
        'message': None,
        'etas': etas_
    }
