import asyncio
import logging
import traceback

import aiohttp.web
from grpc.experimental import aio
from google.protobuf.json_format import MessageToDict

import ray
import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
import ray.services
import ray.utils
from ray.core.generated import core_worker_pb2
from ray.core.generated import core_worker_pb2_grpc
from ray.core.generated import node_manager_pb2
from ray.core.generated import node_manager_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.master
class RayletStats:
    def __init__(self, dashboard_master):
        self.dashboard_master = dashboard_master
        self.nodes = []
        self.stubs = {}

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
                channel = aio.insecure_channel("{}:{}".format(
                        node_ip, node["NodeManagerPort"]))
                stub = node_manager_pb2_grpc.NodeManagerServiceStub(
                        channel)
                self.stubs[node_id] = stub

    @routes.get("/api/kill_actor")
    async def kill_actor(self, req) -> aiohttp.web.Response:
        actor_id = req.query.get("actor_id")
        ip_address = req.query.get("ip_address")
        port = req.query.get("port")
        return await dashboard_utils.json_response(
                await self._kill_actor(actor_id, ip_address, port))

    async def _kill_actor(self, actor_id, ip_address, port):
        channel = aio.insecure_channel("{}:{}".format(ip_address, int(port)))
        stub = core_worker_pb2_grpc.CoreWorkerServiceStub(channel)

        await stub.KillActor(
                core_worker_pb2.KillActorRequest(
                        intended_actor_id=ray.utils.hex_to_binary(actor_id)))
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
                    reply = await stub.GetNodeStats(
                            node_manager_pb2.GetNodeStatsRequest(), timeout=2)
                    reply_dict = MessageToDict(reply)
                    reply_dict["nodeId"] = node_id
                    replies[node["NodeManagerAddress"]] = reply_dict
                for address, reply_dict in replies.items():
                    datacenter.raylet_stats[address] = reply_dict
            except Exception:
                logger.exception(traceback.format_exc())
            finally:
                counter += 1
                # From time to time, check if new nodes have joined the cluster
                # and update self.nodes
                if counter % 10:
                    self._update_nodes()
