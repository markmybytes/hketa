import asyncio
import csv
from datetime import datetime
from typing import Generator, Optional

import aiohttp
import pytz

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta, search_location


@ensure_session
async def routes(*, session: aiohttp.ClientSession) -> dict[str, t.Route]:
    routes_ = {}
    async with session.get(
            'https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv') as request:
        for row in csv.reader((await request.text('utf-8')).splitlines()[1:]):
            # column definition:
            #   route, direction, stopCode, stopID, stopTCName, stopENName, seq
            if not any(row):  # skip empty lines
                continue

            direction, _, branch = row[1].partition('-')
            if branch:
                # route with branch lines
                direction, branch = branch, direction  # e.g. LMC-DT
            direction = 'outbound' if direction == 'DT' else 'inbound'
            routes_.setdefault(row[0], {'inbound': [], 'outbound': []})

            if (row[6] == '1.00'):
                # origin
                routes_[row[0]][direction].append({
                    'id': (route_id := '_'.join(filter(None, (row[0], direction, branch)))),
                    'description': None,
                    'orig': {
                        'id': row[2],
                        'seq': int(row[6].removesuffix('.00')),
                        'name': {'en': row[5], 'tc': row[4]}
                    },
                    'dest': {}
                })
            else:
                # destination
                if len(routes_[row[0]][direction]) == 1:
                    routes_[row[0]][direction][0]['dest'] = {
                        'seq': int(row[6].removesuffix('.00')),
                        'name': {'en': row[5], 'tc': row[4]}
                    }
                else:
                    for idx, branch in enumerate(routes_[row[0]][direction]):
                        if branch['id'] != route_id:
                            continue
                        routes_[row[0]][direction][idx]['dest'] = {
                            'seq': int(row[6].removesuffix('.00')),
                            'name': {'en': row[5], 'tc': row[4]}
                        }
                        break
    return routes_


@ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession) -> Generator[t.Stop, None, None]:
    # column definition:
    #   route, direction, stopCode, stopID, stopTCName, stopENName, seq
    async with session.get(
            'https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv') as request:
        if len(route_id := route_id.split('_')) > 2:
            stops_ = [stop for stop in csv.reader((await request.text('utf-8')).splitlines()[1:])
                      if stop[0] == route_id[0]
                      and stop[1] == f'{route_id[2]}-{"DT" if route_id[1] == "outbound" else "UT"}']
        else:
            stops_ = [stop for stop in csv.reader((await request.text('utf-8')).splitlines()[1:])
                      if stop[0] == route_id[0]
                      and stop[1] == ('DT' if route_id[1] == 'outbound' else 'UT')]

    if len(stops_) == 0:
        raise KeyError('route not exists')

    locations = await asyncio.gather(
        *[search_location(f'\u6e2f\u9435{s[4]}\u7ad9', session) for s in stops_])
    return ({
        'id': s[2],
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
    route, direction, _ = route_id.split('_')
    direction = 'DOWN' if direction == 'outbound' else 'UP'

    async with session.get('https://rt.data.gov.hk/v1/transport/mtr/getSchedule.php',
                           params={
                               'line': route,
                               'sta': stop_id,
                               'lang': 'tc' if language == 'zh' else 'en'
                           }) as request:
        response = await request.json()

    if len(response) == 0:
        return error_eta('api-error')
    if response.get('status', 0) == 0:
        if 'suspended' in response['message']:
            return error_eta(response['message'])
        if response.get('url') is not None:
            return error_eta('ss-effect')
        return error_eta('api-error')

    etas_ = []
    timestamp = datetime.fromisoformat(response['curr_time'])\
        .astimezone(pytz.timezone('Asia/Hong_kong'))

    for entry in response['data'][f'{route}-{stop_id}'].get(direction, []):
        eta_dt = datetime.fromisoformat(entry['time'])\
            .astimezone(pytz.timezone('Asia/Hong_kong'))
        etas_.append({
            'eta': dt_to_8601(eta_dt),
            'is_arriving': (eta_dt - timestamp).total_seconds() < 90,
            'is_scheduled': False,
            'extras': {
                'destination': entry['dest'],
                'varient': _varient_text(entry.get('route'), language),
                'platform': entry['plat'],
                'car_length': None
            },
            'remark': None,
        })

    if len(etas_) == 0:
        return error_eta('empty')
    return {
        'timestamp': dt_to_8601(timestamp),
        'message': None,
        'etas': etas_
    }


def _varient_text(val: Optional[str], language: t.Language):
    if val == 'RAC':
        return '\u7d93\u99ac\u5834' if language == 'zh' else 'Via Racecourse'
    return val
