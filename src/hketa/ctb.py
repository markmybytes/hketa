import asyncio
from datetime import datetime
from typing import Literal, Optional

import aiohttp

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta


# @ensure_session
# async def routes(*, session: aiohttp.ClientSession):
#     # Stop ID of the same stop from different route will have the same ID,
#     # caching the stop details to reduce the number of requests (around 600 - 700).
#     # Execution time is not guaranteed to be reduced.
#     stop_cache = {}
#     semaphore = asyncio.Semaphore(10)

#     async with session.get('https://rt.data.gov.hk/v2/transport/citybus/route/ctb') as response:
#         tasks = [_stop_list(s['route'], stop_cache, semaphore, session)
#                  for s in (await response.json())['data']]
#     return {route[0]: route[1] for route in await asyncio.gather(*tasks)}

@ensure_session
async def routes(*, session: aiohttp.ClientSession):
    async def ends(r: dict, s: aiohttp.ClientSession):
        # pylint: disable=line-too-long
        async with s.get(f'https://rt.data.gov.hk/v2/transport/citybus/route-stop/ctb/{r["route"]}/inbound') as response:
            return r['route'], {
                'outbound': [{
                    'id': f'{r["route"]}_outbound_1',
                    'description': None,
                    'orig': {
                        'zh': r['orig_tc'],
                        'en': r['orig_en']
                    },
                    'dest': {
                        'zh': r['dest_tc'],
                        'en': r['dest_en']
                    },
                }],
                'inbound': [] if len((await response.json())['data']) == 0 else {
                    'id': f'{r["route"]}_inbound_1',
                    'description': None,
                    'orig': {
                        'zh': r['dest_tc'],
                        'en': r['dest_en']
                    },
                    'dest': {
                        'zh': r['orig_tc'],
                        'en': r['orig_en']
                    },

                }
            }

    async with session.get('https://rt.data.gov.hk/v2/transport/citybus/route/ctb') as response:
        return {d[0]: d[1]
                for d in await asyncio.gather(*[ends(r, session)
                                                for r in (await response.json())['data']])
                }


@ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession):
    # pylint: disable=line-too-long
    async with session.get(
            f'https://rt.data.gov.hk/v2/transport/citybus/route-stop/ctb/{"/".join(route_id.split("_")[:2])}') as response:
        stops = (await response.json())['data']
        names = await asyncio.gather(*[_stop_name(s['stop'], session) for s in stops])

    if len(stops) == 0:
        raise KeyError('route not exists')
    return ({
        'id': stop['stop'],
        'seq': int(stop['seq']),
        'name': names[idx]
    } for idx, stop in enumerate(stops))


@ensure_session
async def etas(route_id: str,
               stop_id: str,
               language: t.Language = 'zh',
               *,
               session: aiohttp.ClientSession):
    route, direction, _ = route_id.split('_')
    lc = 'tc' if language == 'zh' else 'en'

    async with session.get(
            f'https://rt.data.gov.hk/v2/transport/citybus/eta/ctb/{stop_id}/{route}') as request:
        response = await request.json()

    if len(response) == 0 or response.get('data') is None:
        return error_eta('api-error')
    if len(response['data']) == 0:
        return error_eta('empty')

    etas = []
    timestamp = datetime.fromisoformat(response['generated_timestamp'])

    for eta in response['data']:
        if eta['dir'].lower() != direction[0]:
            continue
        if eta['eta'] == '':
            # 九巴時段
            etas.append({
                'eta': None,
                'is_arriving': False,
                'is_scheduled': True,
                'extras': {
                    'destinaion': eta[f'dest_{lc}'],
                    'varient': None,
                    'platform': None,
                    'car_length': None
                },
                'remark': eta[f'rmk_{lc}'],
            })
        else:
            eta_dt = datetime.fromisoformat(eta['eta'])
            etas.append({
                'eta': dt_to_8601(eta_dt),
                'is_arriving': (eta_dt - timestamp).total_seconds() < 60,
                'is_scheduled': True,
                'extras': {
                    'destinaion': eta[f'dest_{lc}'],
                    'varient': None,
                    'platform': None,
                    'car_length': None
                },
                'remark': eta[f'rmk_{lc}'],
            })

    return {
        'timestamp': dt_to_8601(timestamp),
        'message': None,
        'etas': etas
    }


async def _stop_name(stop_id: str, session: aiohttp.ClientSession) -> dict[str, str]:
    async with session.get(
            f'https://rt.data.gov.hk/v2/transport/citybus/stop/{stop_id}') as response:
        json = (await response.json())['data']
        return {
            'zh': json.get('name_tc', '未有資料'),
            'en': json.get('name_en', 'N/A')
        }


async def _route_ends(route: str,
                      direction: Literal['inbound', 'outbound'],
                      session: aiohttp.ClientSession) -> Optional[tuple[str, str]]:
    # pylint: disable=line-too-long
    async with session.get(
            f'https://rt.data.gov.hk/v2/transport/citybus/route-stop/ctb/{route}/{direction}') as response:
        stops = (await response.json())['data']
        return None if len(stops) == 0 else (stops[0]['stop'], stops[-1]['stop'])


async def _stop_list(
        route: str, cache: dict, semaphore: asyncio.Semaphore, session: aiohttp.ClientSession):
    async with semaphore:
        ends = await asyncio.gather(_route_ends(route, 'outbound', session),
                                    _route_ends(route, 'inbound', session))

    for direction in ends:
        if direction is None:
            continue
        cache.setdefault(direction[0], await _stop_name(direction[0], session))
        cache.setdefault(direction[1], await _stop_name(direction[1], session))

    return route, {
        'outbound': [] if ends[0] is None else [{
            'id': f'{route}_outbound_1',
            'description': None,
            'orig': cache.get(ends[0][0]),
            'dest': cache.get(ends[0][1]),
        }],
        'inbound': [] if ends[1] is None else [{
            'id': f'{route}_inbound_1',
            'description': None,
            'orig': cache.get(ends[1][0]),
            'dest': cache.get(ends[1][1]),

        }]
    }
