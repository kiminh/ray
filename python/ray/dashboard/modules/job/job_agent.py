import logging

import ray.dashboard.utils as dashboard_utils
from ray.core.generated import job_pb2
from ray.core.generated import job_pb2_grpc

# Logger for this module. It should be configured at the entry point
# into the program using Ray. Ray provides a default configuration at
# entry/init points.
logger = logging.getLogger(__name__)


class JobAgentServer(job_pb2_grpc.JobServiceServicer):
    def __init__(self):
        pass

    async def StartJob(self, request, context):
        return job_pb2.StartJobReply(
                job_id=request.job_id,
                state=job_pb2.JobState.RUNNING)


@dashboard_utils.agent
class JobAgent:
    def __init__(self, dashboard_agent):
        """Initialize the JobAgent object."""
        self.dashboard_agent = dashboard_agent

    async def run(self, server):
        job_pb2_grpc.add_JobServiceServicer_to_server(
                JobAgentServer(), server)
