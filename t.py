import asyncio
import json
import pprint
from difflib import SequenceMatcher
from pathlib import Path

import aiohttp

from src import hketa
from src.hketa import _gtfs_parser, _utils

# with open('./test/nlb.json', 'w', encoding='utf-8') as f:
#     json.dump(asyncio.run(hketa.routes('nlb')),
#               f, indent=4, ensure_ascii=False)

_gtfs_parser._BASE_PATH = Path(__file__).parent.joinpath('test', 'gtfs')

# asyncio.run(_gtfs_parser.gtfs_calendar())
# asyncio.run(_gtfs_parser.gtfs_fares())
# asyncio.run(_gtfs_parser.gtfs_frequencies())
# asyncio.run(_gtfs_parser.gtfs_routes())
# asyncio.run(_gtfs_parser.gtfs_stops())

# for co, routes in asyncio.run(_gtfs_parser.gtfs_routes()).items():
#     for no, varient in routes.items():
#         if len(varient) > 1:
#             print(f"{co}, {no}")


def clean_name(n: str) -> str:
    return n.replace('(循環線)', '').replace('(', '').replace(')', '')


with open('./test/kmb.json', encoding='utf-8') as f:
    routes_kmb = json.load(f)

with open('./test/gtfs/_hketa_gtfs_routes.json', encoding='utf-8') as f:
    all_gtfs = json.load(f)
    routes_gtfs = all_gtfs['kmb'] | all_gtfs['lwb']

matched = asyncio.run(_utils.ensure_session(
    _utils.gtfs_route_match)('kmb', routes_kmb))

for no, bound in matched.items():
    for direction, services in bound.items():
        for service in services:
            if 'gtfs_id' not in service and no in routes_gtfs:
                print(no)

with open('./test/kmb_gtfs.json', 'w', encoding='utf-8') as f:
    json.dump(matched, f, ensure_ascii=False, indent=4)
