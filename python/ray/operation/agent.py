import asyncio
import argparse
import logging
import traceback
import os
from grpc.experimental import aio

import ray
import ray.ray_constants as ray_constants
import ray.utils
import ray.services
import ray.operation.modules.utils as module_utils

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


class OperationAgent(object):
    def __init__(self, redis_address, redis_password=None):
        """Initialize the OperationAgent object."""
        self.redis_address = redis_address
        self.redis_password = redis_password
        self.ip = ray.services.get_node_ip_address()
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)
        self.server = aio.server(options=(("grpc.so_reuseport", 0),))
        self.port = self.server.add_insecure_port("[::]:0")

    def _load_modules(self):
        """Load operation agent modules."""
        agent_cls_list = module_utils.get_all_modules(module_utils.TYPE_AGENT)
        modules = []
        for cls in agent_cls_list:
            logger.info("Load {} module: {}", module_utils.TYPE_AGENT, cls)
            c = cls(redis_address=self.redis_address,
                    redis_password=self.redis_password)
            modules.append(c)
        logger.info("Load {} modules.".format(len(modules)))
        return modules

    async def run(self):
        await self.server.start()
        self.redis_client.set("OPERATION_AGENT_PORT:{}".format(self.ip),
                              self.port)
        modules = self._load_modules()
        for m in modules:
            await m.run(self.server)
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
            type=str,
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
    ray.utils.setup_logger(args.logging_level, args.logging_format)

    try:
        agent = OperationAgent(
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
                redis_client, ray_constants.OPERATION_AGENT_DIED_ERROR, message)
        raise e
