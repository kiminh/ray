import asyncio
import json
import uuid

import grpc
import ray
import ray.operation.modules.utils as module_utils
import ray.services
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc


@module_utils.master
class ReportMaster:
    def __init__(self, redis_address, redis_password=None):
        self.reporter_stubs = {}
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)

        self._profiling_stats = {}

        self._update_stubs()

        super().__init__()

    def _update_stubs(self):
        nodes = ray.nodes()
        node_ids = [node["NodeID"] for node in nodes]

        # First remove node connections of disconnected nodes.
        for node_id in self.reporter_stubs.keys():
            if node_id not in node_ids:
                reporter_stub = self.reporter_stubs.pop(node_id)
                reporter_stub.close()

        # Now add node connections of new nodes.
        for node in nodes:
            node_id = node["NodeID"]
            if node_id not in self.reporter_stubs:
                node_ip = node["NodeManagerAddress"]
                reporter_port = module_utils.get_agent_port(self.redis_client, node_ip)
                if not reporter_port:
                    continue
                reporter_channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, int(reporter_port)))
                reporter_stub = reporter_pb2_grpc.ReporterServiceStub(
                        reporter_channel)
                self.reporter_stubs[node_id] = reporter_stub

    def launch_profiling(self, node_id, pid, duration):
        profiling_id = str(uuid.uuid4())

        def _callback(reply_future):
            reply = reply_future.result()
            self._profiling_stats[profiling_id] = reply

        reporter_stub = self.reporter_stubs[node_id]
        reply_future = reporter_stub.GetProfilingStats.future(
                reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        reply_future.add_done_callback(_callback)
        return profiling_id

    def check_profiling_status(self, profiling_id):
        is_present = profiling_id in self._profiling_stats
        if not is_present:
            return {"status": "pending"}

        reply = self._profiling_stats[profiling_id]
        if reply.stderr:
            return {"status": "error", "error": reply.stderr}
        else:
            return {"status": "finished"}

    def get_profiling_info(self, profiling_id):
        profiling_stats = self._profiling_stats.get(profiling_id)
        assert profiling_stats, "profiling not finished"
        return json.loads(profiling_stats.profiling_stats)

    async def run(self):
        while True:
            await asyncio.sleep(10.0)
            self._update_stubs()
