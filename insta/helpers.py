import asyncio
import codecs
from functools import wraps, partial


def to_json(python_object):
    if isinstance(python_object, bytes):
        return {'__class__': 'bytes',
                '__value__': codecs.encode(python_object, 'base64').decode()}
    raise TypeError(repr(python_object) + ' is not JSON serializable')


def from_json(json_object):
    if '__class__' in json_object and json_object['__class__'] == 'bytes':
        return codecs.decode(json_object['__value__'].encode(), 'base64')
    return json_object


def divide_chunks(l, n):
    # looping till length l
    for i in range(0, len(l), n):
        yield l[i:i + n]


def wrap(func):
    @asyncio.coroutine
    @wraps(func)
    def run(*args, loop=None, executor=None, **kwargs):
        if loop is None:
            loop = asyncio.get_event_loop()
        pfunc = partial(func, *args, **kwargs)
        return loop.run_in_executor(executor, pfunc)

    return run


from copy import deepcopy


def dget(d, way, default=None, no_copy=False):
    way = way.split('.')
    if not d:
        return default
    itr = d
    value = default
    try:
        for key in way:
            if isinstance(itr, list):
                key = int(key)
            itr = itr[key]
        value = itr
    except (KeyError, ValueError, IndexError):
        pass

    if (isinstance(value, object) or isinstance(value, list) or isinstance(value, dict)) and not no_copy:
        value = deepcopy(value)

    return value
