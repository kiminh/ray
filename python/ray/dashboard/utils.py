import asyncio
import datetime
import importlib
import inspect
import logging
import pkgutil
from base64 import b64decode

import aiohttp.web
from grpc.experimental import aio

import ray

TYPE_AGENT = "agent"
TYPE_MASTER = "master"

logger = logging.getLogger(__name__)


def agent(cls):
    cls.__ray_module_type__ = TYPE_AGENT
    return cls


def master(enable):
    def _wrapper(cls):
        if enable:
            cls.__ray_module_type__ = TYPE_MASTER
        return cls

    if inspect.isclass(enable):
        return _wrapper(enable)
    return _wrapper


def get_all_modules(module_type):
    logger.info("get all by type: {}".format(module_type))
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


def get_agent_port(redis_client, node_ip):
    agent_port = redis_client.get(
            "DASHBOARD_AGENT_PORT:{}".format(node_ip))
    return int(agent_port) if agent_port else None


def run_agent(agent):
    server = aio.server(options=(("grpc.so_reuseport", 0),))
    loop = asyncio.get_event_loop()
    server.start()
    loop.create_task(agent.run())
    server.wait_for_termination()
    loop.run_forever()


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
