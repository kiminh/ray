import argparse
import asyncio
import logging
import os
import traceback

from grpc.experimental import aio

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.ray_constants as ray_constants
import ray.services
import ray.utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)

aio.init_grpc_aio()


class DashboardAgent(object):
    def __init__(self, redis_address, redis_password=None):
        """Initialize the DashboardAgent object."""
        self.redis_address = redis_address
        self.redis_password = redis_password
        self.ip = ray.services.get_node_ip_address()
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)
        self.server = aio.server(options=(("grpc.so_reuseport", 0),))
        self.port = self.server.add_insecure_port("[::]:0")

    def _load_modules(self):
        """Load dashboard agent modules."""
        agent_cls_list = dashboard_utils.get_all_modules(dashboard_consts.TYPE_AGENT)
        modules = []
        for cls in agent_cls_list:
            logger.info("Load %s module: %s", dashboard_consts.TYPE_AGENT, cls)
            c = cls(redis_address=self.redis_address,
                    redis_password=self.redis_password)
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
    args = parser.parse_args()
    logging.basicConfig(level=args.logging_level, format=args.logging_format)

    try:
        agent = DashboardAgent(
                args.redis_address, redis_password=args.redis_password)

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
