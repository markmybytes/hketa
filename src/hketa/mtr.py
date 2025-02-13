import csv
from datetime import datetime
from typing import Optional

import aiohttp
import pytz

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta


@ensure_session
async def routes(*, session: aiohttp.ClientSession):
    routes = {}
    async with session.get('https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv') as response:
        for row in csv.reader((await response.text('utf-8')).splitlines()[1:]):
            # column definition:
            #   route, direction, stopCode, stopID, stopTCName, stopENName, seq
            if not any(row):  # skip empty lines
                continue

            direction, _, branch = row[1].partition('-')
            if branch:
                # route with branch lines
                direction, branch = branch, direction  # e.g. LMC-DT
            direction = 'outbound' if direction == 'DT' else 'inbound'
            routes.setdefault(row[0], {'inbound': [], 'outbound': []})

            if (row[6] == '1.00'):
                # origin
                routes[row[0]][direction].append({
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
                if len(routes[row[0]][direction]) == 1:
                    routes[row[0]][direction][0]['dest'] = {
                        'seq': int(row[6].removesuffix('.00')),
                        'name': {'en': row[5], 'tc': row[4]}
                    }
                else:
                    for idx, branch in enumerate(routes[row[0]][direction]):
                        if branch['id'] != route_id:
                            continue
                        routes[row[0]][direction][idx]['dest'] = {
                            'seq': int(row[6].removesuffix('.00')),
                            'name': {'en': row[5], 'tc': row[4]}
                        }
                        break
    return routes


@ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession):
    # column definition:
    #   route, direction, stopCode, stopID, stopTCName, stopENName, seq
    async with session.get('https://opendata.mtr.com.hk/data/mtr_lines_and_stations.csv') as response:
        if len(route_id := route_id.split('_')) > 2:
            stops = [stop for stop in csv.reader((await response.text('utf-8')).splitlines()[1:])
                     if stop[0] == route_id[0]
                     and stop[1] == f'{route_id[2]}-{"DT" if route_id[1] == "outbound" else "UT"}']
        else:
            stops = [stop for stop in csv.reader((await response.text('utf-8')).splitlines()[1:])
                     if stop[0] == route_id[0]
                     and stop[1] == ('DT' if route_id[1] == 'outbound' else 'UT')]

    if len(stops) == 0:
        raise KeyError('route not exists')
    return ({
        'id': s[2],
        'seq': int(s[6].removesuffix('.00')),
        'name': {'zh': s[4], 'en': s[5]}
    } for s in stops)


@ ensure_session
async def etas(route_id: str, stop_id: str, language: t.Language = 'zh', *, session: aiohttp.ClientSession):
    route, direction, _ = route_id.split('_')
    direction = 'DOWN' if direction == 'outbound' else 'UP'

    async with session.get('https://rt.data.gov.hk/v1/transport/mtr/getSchedule.php',
                           params={'line': route, 'sta': stop_id, 'lang': 'tc' if language == 'zh' else 'en'}) as request:
        response = await request.json()

    if len(response) == 0:
        return error_eta('api-error')
    if response.get('status', 0) == 0:
        if 'suspended' in response['message']:
            return error_eta(response['message'])
        if response.get('url') is not None:
            return error_eta('ss-effect')
        return error_eta('api-error')

    etas = []
    timestamp = datetime.fromisoformat(response['curr_time'])\
        .astimezone(pytz.timezone('Asia/Hong_kong'))

    for entry in response['data'][f'{route}-{stop_id}'].get(direction, []):
        eta_dt = datetime.fromisoformat(entry['time'])\
            .astimezone(pytz.timezone('Asia/Hong_kong'))
        etas.append({
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

    if len(etas) == 0:
        return error_eta('empty')
    return {
        'timestamp': dt_to_8601(timestamp),
        'message': None,
        'etas': etas
    }


def _varient_text(val: Optional[str], language: t.Language):
    if val == 'RAC':
        return '\u7d93\u99ac\u5834' if language == 'zh' else 'Via Racecourse'
    return val
