from datetime import datetime
from functools import wraps
from pathlib import Path
import random
from typing import Awaitable, Literal, Union

import aiohttp
import pytz

from . import t

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
