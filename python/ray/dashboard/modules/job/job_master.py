import logging

from google.protobuf.json_format import MessageToDict
from grpc.experimental import aio

import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import job_pb2
from ray.core.generated import job_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.master
class JobMaster:
    def __init__(self, dashboard_master):
        self.dashboard_master = dashboard_master
        self.stubs = {}
        datacenter.agents.signal.append(self._update_stubs)

    @routes.get("/job/start")
    async def start_job(self, req):
        job_id = req.query.get("job_id", "default")
        stub = next(iter(self.stubs.values()))
        reply = await stub.DispatchJobInfo(
                job_pb2.DispatchJobInfoRequest(job_id=job_id))
        return await dashboard_utils.json_response(result=MessageToDict(reply))

    async def _update_stubs(self, change):
        if change.new:
            ip, port = next(iter(change.new.items()))
            channel = aio.insecure_channel("{}:{}".format(
                    ip, int(port)))
            stub = job_pb2_grpc.JobServiceStub(
                    channel)
            self.stubs[ip] = stub
        if change.old:
            ip, port = next(iter(change.new.items()))
            stub = self.stubs.pop(ip)
            stub.close()

    async def run(self):
        pass
