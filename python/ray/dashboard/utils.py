import datetime
import functools
import importlib
import inspect
import logging
import pkgutil
import collections
from base64 import b64decode

import aiohttp.web
from aiohttp import hdrs

import ray
import ray.dashboard.consts as dashboard_consts

logger = logging.getLogger(__name__)


def agent(cls):
    cls.__ray_module_type__ = dashboard_consts.TYPE_AGENT
    return cls


def master(enable):
    def _wrapper(cls):
        if enable:
            cls.__ray_module_type__ = dashboard_consts.TYPE_MASTER
        return cls

    if inspect.isclass(enable):
        return _wrapper(enable)
    return _wrapper


class ClassMethodRouteTable:
    _bind_map = collections.defaultdict(dict)
    _routes = aiohttp.web.RouteTableDef()

    # TODO(fyrestone): use dataclass instead.
    class _BindInfo:
        def __init__(self, filename, lineno, instance):
            self.filename = filename
            self.lineno = lineno
            self.instance = instance

    @classmethod
    def routes(cls):
        return cls._routes

    @classmethod
    def _register_route(cls, method, path, **kwargs):
        def _wrapper(handler):
            if path in cls._bind_map[method]:
                bind_info = cls._bind_map[method][path]
                raise Exception("Duplicated route path: {}, previous one registered at {}:{}".format(
                        path, bind_info.filename, bind_info.lineno))

            bind_info = cls._BindInfo(handler.__code__.co_filename, handler.__code__.co_firstlineno, None)

            @functools.wraps(handler)
            async def _handler_route(*args, **kwargs):
                return await handler(bind_info.instance, *args, **kwargs)

            cls._bind_map[method][path] = bind_info
            _handler_route.__route_method__ = method
            _handler_route.__route_path__ = path
            return cls._routes.route(method, path, **kwargs)(_handler_route)

        return _wrapper

    @classmethod
    def head(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_HEAD, path, **kwargs)

    @classmethod
    def get(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_GET, path, **kwargs)

    @classmethod
    def post(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_POST, path, **kwargs)

    @classmethod
    def put(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PUT, path, **kwargs)

    @classmethod
    def patch(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_PATCH, path, **kwargs)

    @classmethod
    def delete(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_DELETE, path, **kwargs)

    @classmethod
    def view(cls, path, **kwargs):
        return cls._register_route(hdrs.METH_ANY, path, **kwargs)

    @classmethod
    def bind(cls, master_instance):
        def predicate(o):
            if inspect.ismethod(o):
                return hasattr(o, "__route_method__") and hasattr(o, "__route_path__")
            return False

        handler_routes = inspect.getmembers(master_instance, predicate)
        for _, h in handler_routes:
            cls._bind_map[h.__func__.__route_method__][h.__func__.__route_path__].instance = master_instance


def get_all_modules(module_type):
    logger.info("Get all modules by type: {}".format(module_type))
    import ray.dashboard.modules

    result = []
    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.dashboard.modules.__path__, "ray.dashboard.modules."):
        m = importlib.import_module(name)
        for k, v in m.__dict__.items():
            if not k.startswith("_") and inspect.isclass(v):
                mtype = getattr(v, "__ray_module_type__", None)
                if mtype == module_type:
                    result.append(v)
    return result


def to_unix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


def round_resource_value(quantity):
    if quantity.is_integer():
        return int(quantity)
    else:
        return round(quantity, 2)


def format_resource(resource_name, quantity):
    if resource_name == "object_store_memory" or resource_name == "memory":
        # Convert to 50MiB chunks and then to GiB
        quantity = quantity * (50 * 1024 * 1024) / (1024 * 1024 * 1024)
        return "{} GiB".format(round_resource_value(quantity))
    return "{}".format(round_resource_value(quantity))


def format_reply_id(reply):
    if isinstance(reply, dict):
        for k, v in reply.items():
            if isinstance(v, dict) or isinstance(v, list):
                format_reply_id(v)
            else:
                if k.endswith("Id"):
                    v = b64decode(v)
                    reply[k] = ray.utils.binary_to_hex(v)
    elif isinstance(reply, list):
        for item in reply:
            format_reply_id(item)


def measures_to_dict(measures):
    measures_dict = {}
    for measure in measures:
        tags = measure["tags"].split(",")[-1]
        if "intValue" in measure:
            measures_dict[tags] = measure["intValue"]
        elif "doubleValue" in measure:
            measures_dict[tags] = measure["doubleValue"]
    return measures_dict


def b64_decode(reply):
    return b64decode(reply).decode("utf-8")


async def json_response(result=None, error=None,
                        ts=None) -> aiohttp.web.Response:
    if ts is None:
        ts = datetime.datetime.utcnow()

    headers = None

    return aiohttp.web.json_response(
            {
                "result": result,
                "timestamp": to_unix_time(ts),
                "error": error,
            },
            headers=headers)


def to_camel_case(snake_str):
    components = snake_str.split('_')
    # We capitalize the first letter of each component except the first one
    # with the 'title' method and join them together.
    return components[0] + ''.join(x.title() for x in components[1:])


def to_google_json_style(d):
    new_dict = {}
    for k, v in d.items():
        if isinstance(v, dict):
            new_dict[to_camel_case(k)] = to_google_json_style(v)
        elif isinstance(v, list):
            new_list = []
            for i in v:
                if isinstance(i, dict):
                    new_list.append(to_google_json_style(i))
                else:
                    new_list.append(i)
            new_dict[to_camel_case(k)] = new_list
        else:
            new_dict[to_camel_case(k)] = v
    return new_dict


class Bunch(dict):
    """A dict with attribute-access"""

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError(key)

    def __setattr__(self, key, value):
        self.__setitem__(key, value)
