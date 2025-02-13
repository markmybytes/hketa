import csv
import json
import tempfile
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Literal, Union

import aiohttp

from ._utils import ensure_session, is_up_to_date

_BASE_PATH = Path(tempfile.gettempdir())


def _bound_id_conv(bound_id: Literal['1', '2']):
    return 'outbound' if bound_id == '1' else 'inbound'


@ensure_session
async def journey_time(*, session: aiohttp.ClientSession):
    path = _BASE_PATH.joinpath('_hketa_rb.json')
    # path = Path(tempfile.gettempdir()).joinpath('_hketa_rb.xml')

    if (path.exists()
            and await is_up_to_date(path,
                                    'https://static.data.gov.hk/td/routes-fares-xml/DATA_LAST_UPDATED_DATE.csv',
                                    session)):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async with session.get('https://static.data.gov.hk/td/routes-fares-xml/ROUTE_BUS.xml') as request:
        data = {}
        for r in ET.fromstring(await request.text()).iter('ROUTE'):
            co = r.find('COMPANY_CODE').text.lower().split('+')
            r_name = r.find('ROUTE_NAMEC').text

            for c in co:
                data.setdefault(c, {})
                data[c].setdefault(r_name, [])
                data[c][r_name].append({
                    'td_route_id': r.find('ROUTE_ID').text,
                    'orig': r.find('LOC_START_NAMEC').text,
                    'dest': r.find('LOC_END_NAMEC').text,
                    'time': r.find('JOURNEY_TIME').text,
                })

    path.write_text(json.dumps(data, ensure_ascii=False), encoding='utf-8')
    return data


@ensure_session
async def gtfs_routes(*, session: aiohttp.ClientSession):
    path = _BASE_PATH.joinpath('_hketa_gtfs_routes.json')

    if path.exists() and await is_up_to_date(path, 'https://static.data.gov.hk/td/pt-headway-en/DATA_LAST_UPDATED_DATE.csv', session):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    routes = {}
    async with session.get('https://static.data.gov.hk/td/pt-headway-tc/routes.txt') as request:
        for line in (l for l in csv.reader((await request.text()).splitlines()[1:]) if l[4] == '3'):
            for co in line[1].lower().split('+'):
                routes.setdefault(co, {})
                routes[co].setdefault(line[2], [])
                routes[co][line[2]].append({
                    'id': line[0],
                    **dict(zip(('orig', 'dest'), line[3].replace('(循環線)', '').split(' - ')))
                })

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(routes, f)


@ensure_session
async def gtfs_calendar(*, session: aiohttp.ClientSession) -> dict[str, Union[str, list[str]]]:
    path = _BASE_PATH.joinpath('_hketa_gtfs_calendar.json')

    if path.exists() and await is_up_to_date(path, 'https://static.data.gov.hk/td/pt-headway-en/DATA_LAST_UPDATED_DATE.csv', session):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async with session.get('https://static.data.gov.hk/td/pt-headway-tc/calendar.txt') as request:
        calendar = {
            c[0]: {
                'weekday': tuple(1 if d == "1" else 0 for d in c[1:8]),
                'incl': [],
                'excl': []
            } for c in csv.reader((await request.text()).splitlines()[1:])
        }

    async with session.get('https://static.data.gov.hk/td/pt-headway-tc/calendar_dates.txt') as request:
        for d in csv.reader((await request.text()).splitlines()[1:]):
            calendar[d[0]]['incl' if d[2] == '1' else 'excl'].append(d[1])

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(calendar, f)

    return calendar


@ensure_session
async def gtfs_frequencies(*, session: aiohttp.ClientSession):
    path = _BASE_PATH.joinpath('_hketa_gtfs_freq.json')
    # schedules = {}
    # async with session.get('https://static.data.gov.hk/td/pt-headway-tc/trips.txt') as request:
    #     trips = {}
    #     for t in (l[-1].split('-') for l in csv.reader((await request.text()).splitlines()[1:])):
    #         direction = 'outbound' if t[1] == '1' else 'inbound'
    #         trips.setdefault(t[0], {})
    #         trips[t[0]].setdefault(direction, {})
    #         trips[t[0]][direction].setdefault(t[2], [])
    #         trips[t[0]][direction][t[2]].append(t[3])

    async with session.get('https://static.data.gov.hk/td/pt-headway-tc/frequencies.txt') as request:
        freqs = {}
        for freq in csv.reader((await request.text()).splitlines()[1:]):
            rid, bound, sid, _ = freq[0].split('-')
            bound = _bound_id_conv(bound)

            freqs.setdefault(rid, {'outbound': {}, 'inbound': {}})
            freqs[rid][bound].setdefault(sid, [])
            freqs[rid][bound][sid].append({
                'start': freq[1],
                'end': freq[2],
                'interval': freq[3]
            })

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(freqs, f)


@ensure_session
async def gtfs_fares(*, session: aiohttp.ClientSession):
    path = _BASE_PATH.joinpath('_hketa_gtfs_fares.json')

    if path.exists() and await is_up_to_date(path, 'https://static.data.gov.hk/td/pt-headway-en/DATA_LAST_UPDATED_DATE.csv', session):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    async with session.get('https://static.data.gov.hk/td/pt-headway-tc/fare_attributes.txt') as request:
        fares = {}
        for fare in csv.reader((await request.text()).splitlines()[1:]):
            rid, bound, idx_on, idx_off = fare[0].split('-')

            if int(idx_off) - int(idx_on) != 1:
                continue

            fares.setdefault(rid, {'outbound': [], 'inbound': []})
            fares[rid][_bound_id_conv(bound)].append(fare[1])

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(fares, f)


@ensure_session
async def gtfs_stops(*, session: aiohttp.ClientSession):
    path = _BASE_PATH.joinpath('_hketa_gtfs_stops.json')

    if path.exists() and await is_up_to_date(path, 'https://static.data.gov.hk/td/pt-headway-en/DATA_LAST_UPDATED_DATE.csv', session):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)

    def parse_name(names: str):
        parsed = {}
        for name in [n for n in names.split('|') if '+' not in n]:
            if name[0] != '[':
                parsed['_'] = name
            else:
                co = name[name.find('[') + 1:name.find(']')]\
                    .lower()\
                    .replace('lwb', 'kmb')
                parsed[co] = name[name.find(']') + 1:]
        # {co: gtfs_name for name in names.split('|') for co, gtfs_name in [name.split(' ', 1)]}
        return parsed

    async with session.get('https://static.data.gov.hk/td/pt-headway-tc/stops.txt') as request:
        stops = {
            s[0]: {
                'name': parse_name(s[1]),
                'lat': s[2],
                'lng': s[3]
            } for s in csv.reader((await request.text()).splitlines()[1:])
        }

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(stops, f)
