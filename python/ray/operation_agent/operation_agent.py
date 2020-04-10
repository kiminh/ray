import asyncio
import argparse
import logging
import traceback
import os
import grpc
from grpc.experimental import aio

import ray
import ray.ray_constants as ray_constants
import ray.utils
import ray.services
from ray.core.generated import operation_pb2
from ray.core.generated import operation_pb2_grpc

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


class OperationAgentServer(operation_pb2_grpc.OperationAgentServiceServicer):
    def __init__(self):
        pass

    def StartJob(self, request, context):
        pass


class OperationAgent(object):
    def __init__(self, redis_address, redis_password=None):
        """Initialize the OperationAgent object."""
        self.ip = ray.services.get_node_ip_address()
        self.redis_client = ray.services.create_redis_client(
            redis_address, password=redis_password)
        self.server = aio.server(options=(("grpc.so_reuseport", 0), ))
        operation_pb2_grpc.add_OperationAgentServiceServicer_to_server(
            OperationAgentServer(), self.server)
        self.port = self.server.add_insecure_port("[::]:0")

    async def run(self):
        await self.server.start()
        self.redis_client.set("OPERATION_AGENT_PORT:{}".format(self.ip),
                              self.port)
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
        message = ("The reporter on node {} failed with the following "
                   "error:\n{}".format(os.uname()[1], traceback_str))
        ray.utils.push_error_to_driver_through_redis(
            redis_client, ray_constants.REPORTER_DIED_ERROR, message)
        raise e
