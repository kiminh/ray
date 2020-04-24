import logging

from google.protobuf.json_format import MessageToDict
from grpc.experimental import aio as aiogrpc

import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import job_pb2
from ray.core.generated import job_pb2_grpc

import aioredis
import asyncio

from ray.dashboard.modules.job.job_info import JobInfo
import ray.dashboard.modules.job.job_updater as job_updater

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable
DEBUG = False


@dashboard_utils.master
class JobMaster:
    def __init__(self, dashboard_master):
        # The dashboard master.
        self._dashboard_master = dashboard_master
        # The stubs to all job agents.
        self._stubs = {}
        # Register the signal to detect the node changing.
        datacenter.agents.signal.append(self._update_stubs)
        # Redis client
        self._aioredis_client = None

        # How many fixed nodes?
        self._fixed_node_num = 10
        self._submitted_jobs_cache = {}

    @routes.post("/jobs/new")
    async def new_job(self, req):
        job_info = JobInfo.from_request(req.query)
        job_info.id = job_id = await job_updater.next_job_id(self._aioredis_client)
        job_info.state = "submitted"

        # If there is no agents now, let's reject it.
        if len(self._stubs) == 0:
            log_msg = "Failed to submit job because there is no agent connected to job master."
            logger.info(log_msg)
            return await dashboard_utils.json_response(
                    result={
                        "status": "rejected",
                        "msg": log_msg,
                        "job_id": job_id,
                    })

        # We select the first agent to start driver for this job.
        ip_to_start_driver = list(self._stubs.keys())[0]
        job_info.assigned_node_name = ip_to_start_driver

        # Write job info to GCS synchronously.
        await job_updater.submit_job(self._aioredis_client, job_info.to_dict())

        # Dispatch job info to all job agents. Note that here we use a
        # local variable `all_agent_keys` to avoid iters getting changed.
        all_agent_keys = self._stubs.keys()
        for agent_ip in all_agent_keys:
            try:
                agent_stub = self._stubs[agent_ip]
            except KeyError:
                logger.info("The job agent might be lost from job manager. But this is not a "
                            "critical error because job agent will handle it when it restarted.")
                continue

            if agent_ip == ip_to_start_driver:
                reply = await agent_stub.DispatchJobInfo(
                        job_pb2.DispatchJobInfoRequest(job_id=job_id, start_driver=True))
            else:
                reply = await agent_stub.DispatchJobInfo(
                        job_pb2.DispatchJobInfoRequest(job_id=job_id, start_driver=False))
            if reply.status != job_pb2.JobStatus.OK:
                logger.info()
                # Check the reply status and write the records which node has reply.

        logger.info("Succeeded to submit job %s", job_id)
        return await dashboard_utils.json_response(
                result={
                    "status": "ok",
                    "msg": "Succeeded to submit job.",
                    "job_id": job_id,
                })

    async def _update_stubs(self, change):
        if change.new:
            ip, port = next(iter(change.new.items()))
            channel = aiogrpc.insecure_channel("{}:{}".format(
                    ip, int(port)))
            stub = job_pb2_grpc.JobServiceStub(
                    channel)
            self._stubs[ip] = stub
        if change.old:
            ip, port = next(iter(change.new.items()))
            stub = self._stubs.pop(ip)
            stub.close()
        if DEBUG:
            test_job = {
                'id': '',
                'name': 'rayag_darknet',
                'owner': 'abc.xyz',
                'language': 'java',
                'url': 'http://arcos.oss-cn-hangzhou-zmf.aliyuncs.com/po/simple_job.zip',
                'driver_entry': 'simple_job',
                'driver_args': ['arg1', 'arg2'],
                'custom_config': {'k1': 'v1', 'k2': 'v2'},
                'jvm_options': '-Dabc=123 -Daaa=xxx',
                'dependencies': {
                    'python': ['aiohttp', 'click', 'colorama', 'filelock', 'google', 'grpcio', 'jsonschema',
                               'msgpack >= 0.6.0, < 1.0.0', 'numpy >= 1.16', 'protobuf >= 3.8.0', 'py-spy >= 0.2.0',
                               'pyyaml', 'redis >= 3.3.2']
                }
            }
            await self._submit_job(test_job)

    async def run(self):
        self._aioredis_client = await aioredis.create_redis(
                address=self._dashboard_master.redis_address,
                password=self._dashboard_master.redis_password)
