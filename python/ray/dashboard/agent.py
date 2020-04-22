import argparse
import asyncio
import logging
import os
import traceback

from grpc.experimental import aio as aiogrpc

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray.services
import ray.utils

logger = logging.getLogger(__name__)

aiogrpc.init_grpc_aio()


class DashboardAgent(object):
    def __init__(self,
                 raylet_socket_name,
                 store_socket_name,
                 node_ip_address,
                 node_manager_port,
                 redis_address,
                 redis_password,
                 session_dir,
                 temp_dir):
        """Initialize the DashboardAgent object."""
        ip, port = redis_address.split(":")
        self.raylet_socket_name = raylet_socket_name
        self.store_socket_name = store_socket_name
        self.node_ip_address = node_ip_address
        self.node_manager_port = node_manager_port
        self.redis_address = (ip, int(port))
        self.redis_password = redis_password
        self.session_dir = session_dir
        self.temp_dir = temp_dir
        self.ip = ray.services.get_node_ip_address()
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)
        self.server = aiogrpc.server(options=(("grpc.so_reuseport", 0),))
        self.port = self.server.add_insecure_port("[::]:0")

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
            "--raylet-socket-name",
            required=True,
            type=str,
            help="The socket path of the raylet process")
    parser.add_argument(
            "--store-socket-name",
            required=True,
            type=str,
            help="The socket name of the plasma store")
    parser.add_argument(
            "--node-ip-address",
            required=True,
            type=str,
            help="The ip address to use for connecting the node manager")
    parser.add_argument(
            "--node-manager-port",
            required=True,
            type=int,
            help="The port to use for connecting the node manager")
    parser.add_argument(
            "--redis-address",
            required=True,
            type=str,
            help="The address to use for Redis.")
    parser.add_argument(
            "--redis-password",
            required=False,
            type=str,
            default=None,
            help="The password to use for Redis")
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
            "--session-dir",
            required=False,
            default=None,
            help="The session dir of the Ray process")
    parser.add_argument(
            "--temp-dir",
            required=False,
            default=None,
            help="The root temporary dir of the Ray process")
    args = parser.parse_args()
    logging.basicConfig(level=args.logging_level, format=args.logging_format)

    try:
        agent = DashboardAgent(
                raylet_socket_name=args.raylet_socket_name,
                store_socket_name=args.store_socket_name,
                node_ip_address=args.node_ip_address,
                node_manager_port=args.node_manager_port,
                redis_address=args.redis_address,
                redis_password=args.redis_password,
                session_dir=args.session_dir,
                temp_dir=args.temp_dir)

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
