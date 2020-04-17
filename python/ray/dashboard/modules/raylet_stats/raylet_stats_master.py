import asyncio
import copy
import logging
import traceback
from typing import Dict

import grpc
from google.protobuf.json_format import MessageToDict

import ray
import ray.dashboard.utils as dashboard_utils
import ray.services
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc

import aiohttp.web

logger = logging.getLogger(__name__)
routes = aiohttp.web.RouteTableDef()


@dashboard_utils.master
class RayletStats:
    def __init__(self, redis_address, redis_password=None):
        self.nodes = []
        self.stubs = {}
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)

        self._raylet_stats = {}
        self._profiling_stats = {}

        self._update_nodes()

        super().__init__()

    def _update_nodes(self):
        self.nodes = ray.nodes()
        node_ids = [node["NodeID"] for node in self.nodes]

        # First remove node connections of disconnected nodes.
        for node_id in self.stubs.keys():
            if node_id not in node_ids:
                stub = self.stubs.pop(node_id)
                stub.close()

        # Now add node connections of new nodes.
        for node in self.nodes:
            node_id = node["NodeID"]
            if node_id not in self.stubs:
                node_ip = node["NodeManagerAddress"]
                channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, node["NodeManagerPort"]))
                stub = node_manager_pb2_grpc.NodeManagerServiceStub(
                        channel)
                self.stubs[node_id] = stub

    def get_raylet_stats(self) -> Dict:
        return copy.deepcopy(self._raylet_stats)

    @routes.get("/api/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        actor_id = req.query.get("actor_id")
        ip_address = req.query.get("ip_address")
        port = req.query.get("port")
        return await dashboard_utils.json_response(
                self._kill_actor(actor_id, ip_address, port))

    def _kill_actor(self, actor_id, ip_address, port):
        channel = grpc.insecure_channel("{}:{}".format(ip_address, int(port)))
        stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

        def _callback(reply_future):
            _ = reply_future.result()

        reply_future = stub.KillActor.future(
                core_worker_pb2.KillActorRequest(
                        intended_actor_id=ray.utils.hex_to_binary(actor_id)))
        reply_future.add_done_callback(_callback)
        return {}

    async def run(self):
        counter = 0
        while True:
            await asyncio.sleep(1.0)
            replies = {}

            try:
                for node in self.nodes:
                    node_id = node["NodeID"]
                    stub = self.stubs[node_id]
                    reply = stub.GetNodeStats(
                            node_manager_pb2.GetNodeStatsRequest(), timeout=2)
                    reply_dict = MessageToDict(reply)
                    reply_dict["nodeId"] = node_id
                    replies[node["NodeManagerAddress"]] = reply_dict
                for address, reply_dict in replies.items():
                    self._raylet_stats[address] = reply_dict
            except Exception:
                logger.exception(traceback.format_exc())
            finally:
                counter += 1
                # From time to time, check if new nodes have joined the cluster
                # and update self.nodes
                if counter % 10:
                    self._update_nodes()
