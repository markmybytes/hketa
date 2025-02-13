from datetime import datetime
from typing import Generator

import aiohttp
import bs4
import pytz

from . import t
from ._utils import dt_to_8601, ensure_session, error_eta, ua_header


@ensure_session
async def routes(*, session: aiohttp.ClientSession) -> dict[str, t.Route]:
    def description(route: dict[str,]):
        descr = {}
        for lc, routes_ in descriptions.items():
            for service in routes_[route['routeNo']]:
                if service['route_name'] == route[f'routeName_{lc}']:
                    descr[lc] = service['description']
                    break
        return descr or None

    descriptions = {'e': {}, 'c': {}}
    async with aiohttp.ClientSession(headers=ua_header()) as sess_cua:
        for lc in ('e', 'c'):
            async with sess_cua.get(
                    f'https://www.nlb.com.hk/language/set/{"en" if lc == "e" else "zh"}'):
                pass
            async with sess_cua.get('https://www.nlb.com.hk/route') as request:
                bs = bs4.BeautifulSoup(await request.text(), "html.parser")

            for tr in bs.select('table.property-table tr')[1:]:
                route_no = tr.select('td')[0].get_text(strip=True)

                descriptions[lc].setdefault(route_no, [])
                descriptions[lc][route_no].append({
                    'route_name': tr.select('td')[1].contents[1].get_text(),
                    'description': tr.select('td')[1].contents[2].get_text(strip=True)
                })

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
async def stops(route_id: str, *, session: aiohttp.ClientSession) -> Generator[t.Stop, None, None]:
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
        },
        'location': (stop['latitude'], stop['longitude'])
    } for idx, stop in enumerate(stops_))


@ensure_session
async def etas(route_id: str,
               stop_id: str,
               language: t.Language = 'zh',
               *,
               session: aiohttp.ClientSession) -> t.Etas:
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
