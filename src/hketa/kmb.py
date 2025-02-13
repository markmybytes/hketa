import asyncio
from datetime import datetime
from itertools import chain
from typing import Generator, Literal, Optional

import aiohttp

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta


@ensure_session
async def routes(*, session: aiohttp.ClientSession) -> dict[str, t.Route]:
    routes_ = {}
    specials = set()

    async with session.get('https://data.etabus.gov.hk/v1/transport/kmb/route') as request:
        for route in (await request.json())['data']:
            routes_.setdefault(route['route'], {'inbound': [], 'outbound': []})
            direction = 'outbound' if route['bound'] == 'O' else 'inbound'

            routes_[route['route']][direction].append({
                'id': f'{route["route"]}_{direction}_{route["service_type"]}',
                'description': None,
                'orig': {'zh': route['orig_tc'], 'en': route['orig_en']},
                'dest': {'zh': route['dest_tc'], 'en': route['dest_en']},
            })

            if len(routes_[route['route']][direction]) > 1:
                specials.add(
                    (route['route'], '1' if route['bound'] == 'O' else '2'))

    varients = chain(*(await asyncio.gather(*[_variants(r, d, session) for r, d in specials])))
    for varient in (v for v in varients if v['ServiceType'] != '01   '):
        # pylint: disable=line-too-long
        for service in routes_[varient['Route']]['outbound' if varient['Bound'] == '1' else 'inbound']:
            if service['id'].split('_')[2] == varient['ServiceType'].strip().removeprefix('0'):
                service['description'] = {
                    'zh': varient['Desc_CHI'],
                    'en': varient['Desc_ENG']
                }
                break
    return routes_


@ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession) -> list[dict[str,]]:
    async def fetch(stop: dict, session: aiohttp.ClientSession):
        async with session.get(
                f'https://data.etabus.gov.hk/v1/transport/kmb/stop/{stop["stop"]}') as request:
            detail = (await request.json())['data']
            return {
                'id': stop['stop'],
                'seq': int(stop['seq']),
                'name': {
                    'tc': detail.get('name_tc'),
                    'en': detail.get('name_en'),
                },
                'location': (detail['lat'], detail['long'])
            }

    # pylint: disable=line-too-long
    async with session.get(
            f'https://data.etabus.gov.hk/v1/transport/kmb/route-stop/{"/".join(route_id.split("_"))}') as request:
        data = await asyncio.gather(
            *[fetch(stop, session) for stop in (await request.json())['data']])

    if len(data) == 0:
        raise KeyError('route not exists')
    return data


@ensure_session
async def etas(route_id: str,
               stop_id: str,
               language: t.Language = 'zh',
               *,
               session: aiohttp.ClientSession) -> t.Etas:
    route, direction, service_type = route_id.split('_')
    lc = 'tc' if language == 'zh' else 'en'

    # pylint: disable=line-too-long
    async with session.get(
            f'https://data.etabus.gov.hk/v1/transport/kmb/eta/{stop_id}/{route}/{service_type}') as request:
        response = await request.json()

    if len(response) == 0:
        return error_eta('api-error', language=language)
    if response.get('data') is None:
        return error_eta('empty', language=language)

    etas_ = []
    timestamp = datetime.fromisoformat(response['generated_timestamp'])

    for eta in response['data']:
        if eta['dir'].lower() != direction[0]:
            continue
        if eta['eta'] is None:
            if eta['rmk_en'] == 'The final bus has departed from this stop':
                return error_eta('eos')
            elif eta['rmk_en'] == '':
                return error_eta('empty')
            return error_eta(eta[f'rmk_{lc}'])

        eta_dt = datetime.fromisoformat(eta['eta'])
        etas_.append({
            'eta': dt_to_8601(eta_dt),
            'is_arriving': (eta_dt - timestamp).total_seconds() < 30,
            'is_scheduled': eta.get(f'rmk_{lc}') in ('\u539f\u5b9a\u73ed\u6b21', 'Scheduled Bus'),
            'extras': {
                'destinaion': eta[f'dest_{lc}'],
                'varient': _varient_text(eta['service_type'], language),
                'platform': None,
                'car_length': None
            },
            'remark': eta[f'rmk_{lc}'],
        })

    return {
        'timestamp': dt_to_8601(timestamp),
        'message': None,
        'etas': etas_
    }


async def _variants(route: str,
                    direction: Literal['1', '2'],
                    session: aiohttp.ClientSession) -> list[dict]:
    async with session.request('GET',
                               'https://search.kmb.hk/KMBWebSite/Function/FunctionRequest.ashx',
                               params={
                                   'action': 'getSpecialRoute',
                                   'route': route,
                                   'bound': direction
                               },
                               ) as requset:
        return (await requset.json(content_type=None))['data']['routes']


def _varient_text(service_type: str, language: t.Language) -> Optional[str]:
    if service_type == '1':
        return None
    return '\u7279\u5225\u73ed\u6b21' if language == 'zh' else 'Special Departure'
