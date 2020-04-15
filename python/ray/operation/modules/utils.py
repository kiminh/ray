import types
import asyncio
import logging
import importlib
import pkgutil
import inspect
from grpc.experimental import aio

TYPE_AGENT = "agent"
TYPE_MASTER = "master"

logger = logging.getLogger(__name__)


def agent(cls):
    cls.__ray_module_type__ = TYPE_AGENT
    return cls


def master(cls):
    cls.__ray_module_type__ = TYPE_MASTER
    return cls


def get_all_modules(module_type):
    logger.info("get all by type: {}".format(module_type))
    import ray.operation.modules

    result = []
    for module_loader, name, ispkg in pkgutil.walk_packages(
            ray.operation.modules.__path__, "ray.operation.modules."):
        m = importlib.import_module(name)
        for k, v in m.__dict__.items():
            if not k.startswith("_") and inspect.isclass(v):
                mtype = getattr(v, "__ray_module_type__", None)
                if mtype == module_type:
                    result.append(v)
    return result


def get_agent_port(redis_client, node_ip):
    agent_port = redis_client.get(
            "OPERATION_AGENT_PORT:{}".format(node_ip))
    return int(agent_port) if agent_port else None


def run_agent(agent):
    server = aio.server(options=(("grpc.so_reuseport", 0),))
    loop = asyncio.get_event_loop()
    server.start()
    loop.create_task(agent.run())
    server.wait_for_termination()
    loop.run_forever()
