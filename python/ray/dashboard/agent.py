import argparse
import asyncio
import logging
import os
import sys
import traceback
import functools
import ipaddress

from grpc.experimental import aio as aiogrpc

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray.services
import ray.utils

logger = logging.getLogger(__name__)

os.environ["GRPC_ENABLE_FORK_SUPPORT"] = "False"
aiogrpc.init_grpc_aio()

GRPC_FORCE_IPV4 = os.environ.get("GRPC_FORCE_IPV4")
if GRPC_FORCE_IPV4 is None:
    GRPC_FORCE_IPV4 = sys.platform == "linux"


def _get_ip_address():
    import socket
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip


def _force_ipv4(bind_ip, host):
    logger.warning("_force_ipv4: %s replaced with %s", host, bind_ip)
    return ipaddress.IPv4Address(bind_ip)


LISTEN_ADDRESS = "[::]:0"
if GRPC_FORCE_IPV4:
    logger.warning("GRPC_FORCE_IPV4: true")
    ip = _get_ip_address()
    ipaddress.ip_address = functools.partial(_force_ipv4, ip)
    LISTEN_ADDRESS = "{}:0".format(ip)
else:
    logger.warning("GRPC_FORCE_IPV4: false")


class DashboardAgent(object):
    def __init__(self, redis_address, redis_password=None, temp_dir=None, node_manager_port=None,
                 object_store_name=None, raylet_name=None):
        """Initialize the DashboardAgent object."""
        ip, port = redis_address.split(":")
        self.redis_address = (ip, int(port))
        self.redis_password = redis_password
        self.temp_dir = temp_dir
        self.node_manager_port = node_manager_port
        self.object_store_name = object_store_name
        self.raylet_name = raylet_name
        self.ip = ray.services.get_node_ip_address()
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        logger.info("Dashboard agent listen at: %s", LISTEN_ADDRESS)
        self.port = self.server.add_insecure_port(LISTEN_ADDRESS)

    def _load_modules(self):
        """Load dashboard agent modules."""
        agent_cls_list = dashboard_utils.get_all_modules(dashboard_consts.TYPE_AGENT)
        modules = []
        for cls in agent_cls_list:
            logger.info("Load %s module: %s", dashboard_consts.TYPE_AGENT, cls)
            c = cls(self)
            modules.append(c)
        logger.info("Load {} modules.".format(len(modules)))
        return modules

    async def run(self):
        await self.server.start()
        self.redis_client.set("{}{}".format(dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX, self.ip),
                              self.port)
        modules = self._load_modules()
        await asyncio.gather(*(m.run(self.server) for m in modules))
        await self.server.wait_for_termination()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
            description=("Parse Redis server for the "
                         "reporter to connect to."))
    parser.add_argument(
            "--redis-address",
            required=True,
            type=str,
            help="The address to use for Redis.")
    parser.add_argument(
            "--node-manager-port",
            required=True,
            type=int,
            help="the port to use for starting the node manager")
    parser.add_argument(
            "--object-store-name",
            required=True,
            type=str,
            default=None,
            help="manually specify the socket name of the plasma store")
    parser.add_argument(
            "--raylet-name",
            required=True,
            type=str,
            default=None,
            help="manually specify the socket path of the raylet process")
    parser.add_argument(
            "--redis-password",
            required=False,
            type=str,
            default=None,
            help="the password to use for Redis")
    parser.add_argument(
            "--logging-level",
            required=False,
            type=lambda s: logging.getLevelName(s.upper()),
            default=ray_constants.LOGGER_LEVEL,
            choices=ray_constants.LOGGER_LEVEL_CHOICES,
            help=ray_constants.LOGGER_LEVEL_HELP)
    parser.add_argument(
            "--logging-format",
            required=False,
            type=str,
            default=ray_constants.LOGGER_FORMAT,
            help=ray_constants.LOGGER_FORMAT_HELP)
    parser.add_argument(
            "--temp-dir",
            required=False,
            type=str,
            default=None,
            help="Specify the path of the temporary directory use by Ray process.")
    args = parser.parse_args()
    logging.basicConfig(level=args.logging_level, format=args.logging_format)

    try:
        if args.temp_dir:
            temp_dir = "/" + args.temp_dir.strip("/")
        else:
            temp_dir = "/tmp/ray"
        agent = DashboardAgent(
                args.redis_address, redis_password=args.redis_password,
                temp_dir=temp_dir, node_manager_port=args.node_manager_port,
                object_store_name=args.object_store_name,
                raylet_name=args.raylet_name)

        loop = asyncio.get_event_loop()
        loop.create_task(agent.run())
        loop.run_forever()
    except Exception as e:
        # Something went wrong, so push an error to all drivers.
        redis_client = ray.services.create_redis_client(
                args.redis_address, password=args.redis_password)
        traceback_str = ray.utils.format_error_message(traceback.format_exc())
        message = ("The agent on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
                redis_client, ray_constants.DASHBOARD_AGENT_DIED_ERROR, message)
        raise e
