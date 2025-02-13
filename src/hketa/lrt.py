import asyncio
import csv
from datetime import datetime, timedelta
from typing import Generator

import aiohttp
import pytz

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta, search_location


@ensure_session
async def routes(*, session: aiohttp.ClientSession) -> dict[str, t.Route]:
    routes_ = {}
    async with session.get(
            'https://opendata.mtr.com.hk/data/light_rail_routes_and_stops.csv') as request:
        for row in csv.reader((await request.text('utf-8')).splitlines()[1:]):
            # column definition:
            #   route, direction, stopCode, stopID, stopTCName, stopENName, seq
            direction = 'outbound' if row[1] == '1' else 'inbound'
            routes_.setdefault(row[0], {'outbound': [], 'inbound': []})

            if row[6] == '1.00':
                # original
                routes_[row[0]][direction].append({
                    'id': None,
                    'description': None,
                    'orig': {
                        'seq': int(row[6].removesuffix('.00')),
                        'name': {'en': row[5], 'tc': row[4]}
                    },
                    'dest': {}
                })
            else:
                # destination
                if row[0] in ('705', '706'):
                    routes_[row[0]][direction][0]['id'] =\
                        f'{row[0]}_{direction}_TSW Circular'
                else:
                    routes_[row[0]][direction][0]['id'] =\
                        f'{row[0]}_{direction}_{row[5]}'

                routes_[row[0]][direction][0]['dest'] = {
                    'seq': int(row[6].removesuffix('.00')),
                    'name': {'en': row[5], 'tc': row[4]}
                }
    return routes_


@ ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession) -> Generator[t.Stop, None, None]:
    async with session.get(
            'https://opendata.mtr.com.hk/data/light_rail_routes_and_stops.csv') as request:
        # pylint: disable=line-too-long
        stops_ = [stop for stop in csv.reader((await request.text('utf-8')).splitlines()[1:])
                  if set((stop[0], 'outbound' if stop[1] == '1' else 'inbound')) == set(route_id.split('_')[:2])]

    if len(stops_) == 0:
        raise KeyError('route not exists')

    locations = await asyncio.gather(
        *[search_location(f'\u8f15\u9435\uff0d{s[4]}', session) for s in stops_])
    return ({
        'id': s[3],
        'seq': int(s[6].removesuffix('.00')),
        'name': {'zh': s[4], 'en': s[5]},
        'location': locations[i]
    } for i, s in enumerate(stops_))


@ ensure_session
async def etas(route_id: str,
               stop_id: str,
               language: t.Language = 'zh',
               *,
               session: aiohttp.ClientSession) -> t.Etas:
    route, _, destination = route_id.split('_')
    lc = 'ch' if language == 'zh' else 'en'

    async with session.get('https://rt.data.gov.hk/v1/transport/mtr/lrt/getSchedule',
                           params={'station_id': stop_id}) as request:
        response = await request.json()

    if len(response) == 0 or response.get('status', 0) == 0:
        return error_eta('api-error')
    if all(platform.get('end_service_status', False)
            for platform in response['platform_list']):
        return error_eta('eos')

    etas_ = []
    cnt_stopped = 0
    timestamp = datetime.fromisoformat(response['system_time'])\
        .astimezone(pytz.timezone('Asia/Hong_kong'))

    for platform in response['platform_list']:
        for eta in platform.get('route_list', []):
            # NOTE: 751P have no destination and eta
            if eta['route_no'] != route:
                continue
            if eta.get('stop') == 1:
                cnt_stopped += 1
                continue
            if eta['dest_en'] != destination:
                continue

            eta_min: str = eta[f'time_{lc}'].split(' ')[0]  # e.g. 3 分鐘 / 即將抵達
            if eta_min.isnumeric():
                etas_.append({
                    'eta': dt_to_8601(
                        timestamp + timedelta(minutes=float(eta_min))),
                    'is_arriving': False,
                    'is_scheduled': False,
                    'extras': {
                        'destination': eta[f'dest_{lc}'],
                        'varient': None,
                        'platform': str(platform['platform_id']),
                        'car_length': eta['train_length']
                    },
                })
            else:
                etas_.append({
                    'eta': dt_to_8601(timestamp),
                    'is_arriving': True,
                    'is_scheduled': False,
                    'remark': eta_min,
                    'extras': {
                        'destination': eta[f'dest_{lc}'],
                        'varient': None,
                        'platform': str(platform['platform_id']),
                        'car_length': eta['train_length']
                    }
                })

    if len(etas_) > 0:
        return {
            'timestamp': dt_to_8601(timestamp),
            'message': None,
            'etas': etas_
        }
    if 'red_alert_status' in response.keys():
        return error_eta(response[f'red_alert_message_{lc}'])
    if cnt_stopped > 0:
        return error_eta('eos')
    return error_eta('empty')
