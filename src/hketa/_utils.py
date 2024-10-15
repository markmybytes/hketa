import random
from copy import deepcopy
from datetime import datetime
from difflib import SequenceMatcher
from functools import wraps
from pathlib import Path
import string
from typing import Awaitable, Literal, Union

import aiohttp
import pyproj
import pytz

from . import _gtfs_parser, t

with open(Path(__file__).parent.joinpath('ua.txt'), encoding='utf-8') as f:
    USER_AGENTS = tuple(a.strip() for a in f.readline())

ERR_MESSAGES = {
    'api-error': {
        'zh': 'API 錯誤',
        'en': 'API Error',
    },
    'empty': {
        'zh': '沒有預報',
        'en': 'No Data',
    },
    'eos': {
        'zh': '服務時間已過',
        'en': 'Not in Service',
    },
    'ss-effect': {
        'zh': '特別車務安排',
        'en': 'Special Service in Effect',
    }
}

EPSG_TRANSFORMER = pyproj.Transformer.from_crs('epsg:2326', 'epsg:4326')


def ensure_session(func: Awaitable):
    @wraps(func)
    async def wrapper(*args, **kwargs):
        if kwargs.get('session') is not None:
            assert isinstance(kwargs['session'], aiohttp.ClientSession)
            return await func(*args, **kwargs)
        async with aiohttp.ClientSession() as s:
            return await func(*args, **{**kwargs, 'session': s})
    return wrapper


def dt_to_8601(dt: datetime) -> str:
    '''Convert a `datetime` instance to ISO-8601 formatted string.'''
    return dt.isoformat(sep='T', timespec='seconds')


def timestamp():
    return datetime.now().replace(tzinfo=pytz.timezone('Etc/GMT-8'))


def error_eta(message: Union[Literal['api-error', 'empty', 'eos', 'ss-effect'], str],
              ts: datetime = None,
              language: t.Language = 'zh'):
    return {
        'timestamp': dt_to_8601(ts or timestamp()),
        'message': ERR_MESSAGES.get(message, {}).get(language, message),
        'etas': None
    }


def ua_header():
    return {'User-Agent': random.choice(USER_AGENTS)}


async def search_location(name: str, session: aiohttp.ClientSession) -> tuple[str, str]:
    async with session.get(
            f'https://geodata.gov.hk/gs/api/v1.0.0/locationSearch?q={name}') as request:
        first = (await request.json())[0]
        return EPSG_TRANSFORMER.transform(first['y'], first['x'])


async def is_up_to_date(path: Path, url: str, session: aiohttp.ClientSession) -> bool:
    async with session.get(url) as request:
        return path.stat().st_mtime >= datetime.strptime((await request.text()).split('\n')[1], '%Y-%m-%d').timestamp()


async def gtfs_route_match(transport: t.Transport,
                           routes: t.Route,
                           session: aiohttp.ClientSession) -> t.Route:
    def similarity(a: str, b: str) -> float:
        return SequenceMatcher(None, a, b).quick_ratio()

    def clean_name(s: str) -> str:
        return s.replace('(循環線)', '')\
            .translate(str.maketrans('', '', string.punctuation))\
            .upper()

    def direction_aware(gtfs_services: list[dict]) -> bool:
        pairs = []
        for service in gtfs_services:
            if ((service['dest'], service['orig']) in pairs):
                return True
            pairs.append((service['orig'], service['dest']))
        return False

    routes = deepcopy(routes)

    _gtfs_data = (await _gtfs_parser.gtfs_routes(session=session))
    routes_gtfs = _gtfs_data['kmb'] | _gtfs_data['lwb']

    for no, bound in routes.items():
        if no not in routes_gtfs:
            continue
        for direction, services in bound.items():
            if len(services) == 1 and len(routes_gtfs[no]) == 1:
                routes[no][direction][0]['gtfs_id'] = routes_gtfs[no][0]['id']
                continue

            matched_ids = set()
            is_multiple = len(routes_gtfs[no]) > 1

            # same orig & dest (272A)
            # Different ID in opposite dir (960X)
            # Via (28B, 30X)
            # Special orig & dest matched (296M)
            for idx, service in enumerate(services):
                for service_gtfs in routes_gtfs[no]:
                    if service_gtfs['id'] in matched_ids:
                        continue

                    if idx == 0 and not direction_aware(routes_gtfs[no]):
                        routes[no][direction][0]['gtfs_id'] = routes_gtfs[no][0]['id']
                        matched_ids.add(routes_gtfs[no][0]['id'])
                        break

                    if (similarity(clean_name(service['orig']['zh']), service_gtfs['orig']) >= 0.6
                            and similarity(clean_name(service['dest']['zh']), service_gtfs['dest']) >= 0.6):
                        routes[no][direction][idx]['gtfs_id'] = service_gtfs['id']
                        matched_ids.add(service_gtfs['id'])
                        break

                    if not idx >= 1:
                        continue

                    if (similarity(clean_name(service['description']['zh']), clean_name(service_gtfs['orig'])) > 0
                            or similarity(clean_name(service['description']['zh']), clean_name(service_gtfs['dest'])) > 0):
                        routes[no][direction][idx]['gtfs_id'] = service_gtfs['id']
                        matched_ids.add(service_gtfs['id'])
                        break

                if 'gtfs_id' in routes[no][direction][idx]:
                    continue

                for service_gtfs in routes_gtfs[no]:
                    if service_gtfs['id'] in matched_ids:
                        continue
                    service_gtfs['orig'] = clean_name(service_gtfs['orig'])
                    service_gtfs['dest'] = clean_name(service_gtfs['dest'])

                    if (similarity(service['dest']['zh'], service_gtfs['orig']) >= 0.6
                            and similarity(service['orig']['zh'], service_gtfs['dest']) >= 0.6):
                        routes[no][direction][idx]['gtfs_id'] = service_gtfs['id']
                        matched_ids.add(service_gtfs['id'])
    return routes
