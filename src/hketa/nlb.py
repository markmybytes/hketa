from datetime import datetime
import re

import aiohttp
import pytz

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta


@ensure_session
async def routes(*, session: aiohttp.ClientSession):
    def description(route: dict[str,]):
        zh, en = [], []
        if route['specialRoute'] == 1:
            zh.append('\u7279\u5225\u7dda')
            en.append('Special Departure')
        if 'Circular' in route['routeName_e']:
            zh.append('\u5faa\u74b0\u7dda')
            en.append('Circular')
        if '(from' in route['routeName_e'] or '(to' in route['routeName_e']:
            s_en = re.search(r'\((from|to)\s(.*?)\)', route['routeName_e'])
            s_zh = re.search(r'\((至)(.*?)\)|\((.*?)(開)\)',
                             route['routeName_c'])
            zh.append(f'{s_zh[1] or ""}{s_zh[2] or s_zh[3]}{s_zh[4] or ""}')
            en.append(f'{s_en[1].capitalize()} {s_en[2]}')

        return None if len(en) == 0 else {
            'zh': '﹐'.join(zh),
            'en': ', '.join(en)
        }

    routes_ = {}
    async with session.get(
            'https://rt.data.gov.hk/v2/transport/nlb/route.php?action=list') as request:
        for route in (await request.json())['routes']:
            routes_.setdefault(route['routeNo'],
                               {'outbound': [], 'inbound': []})
            direction = ('inbound'
                         if len(routes_[route['routeNo']]['outbound'])
                         else 'outbound')
            detail = {
                'description': description(route),
                'orig': {
                    'zh': route['routeName_c'].split(' \u003E ')[0],
                    'en': route['routeName_e'].split(' \u003E ')[0],
                },
                'dest': {
                    'zh': route['routeName_c'].split(' \u003E ')[1],
                    'en': route['routeName_e'].split(' \u003E ')[1],
                }
            }

            # when both the `outbound` and `inbound` have data, it is a special route.
            if all(len(b) for b in routes_[route['routeNo']].values()):
                for bound, parent_rt in routes_[route['routeNo']].items():
                    for r in parent_rt:
                        # special routes usually only differ from either orig or dest stop
                        if (r['orig']['en'] == detail['orig']['en']
                                or r['dest']['en'] == detail['dest']['en']):
                            direction = bound
                            break
                    else:
                        continue
                    break

            routes_[route['routeNo']][direction].append({
                'id': f'{route["routeNo"]}_{direction}_{route["routeId"]}',
                **detail
            })
    return routes_


@ensure_session
async def stops(route_id: str, *, session: aiohttp.ClientSession):
    # pylint: disable=line-too-long
    async with session.get(
            f'https://rt.data.gov.hk/v2/transport/nlb/stop.php?action=list&routeId={route_id.split("_")[-1]}') as request:
        if len(stops_ := (await request.json())['stops']) == 0:
            raise KeyError('route not exists')

    return ({
        'id': stop['stopId'],
        'seq': idx,
        'name': {
            'zh': stop['stopName_c'],
            'en': stop['stopName_e']
        }
    } for idx, stop in enumerate(stops_))


@ensure_session
async def etas(route_id: str,
               stop_id: str,
               language: t.Language = 'zh',
               *,
               session: aiohttp.ClientSession):
    async with session.get('https://rt.data.gov.hk/v2/transport/nlb/stop.php',
                           params={
                               'action': 'estimatedArrivals',
                               'routeId': route_id.split('_')[-1],
                               'stopId': stop_id,
                               'language': language,
                           }) as request:
        response = await request.json()

    if len(response) == 0:
        # incorrect parameter will result in a empty json response
        return error_eta('api-error')
    if not response.get('estimatedArrivals', []):
        return error_eta('empty')

    etas_ = []
    timestamp = datetime.now().replace(tzinfo=pytz.timezone('Etc/GMT-8'))

    for eta in response['estimatedArrivals']:
        eta_dt = datetime.fromisoformat(eta['estimatedArrivalTime']) \
            .astimezone(pytz.timezone('Asia/Hong_kong'))

        etas_.append({
            'eta': dt_to_8601(eta_dt),
            'is_arriving': (eta_dt - timestamp).total_seconds() < 60,
            'is_scheduled': not (eta.get('departed') == '1'
                                 and eta.get('noGPS') == '1'),
            'extras': {
                'destinaion': None,
                'varient': eta.get('routeVariantName'),
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
