import logging
import datetime
import time
import copy
from base64 import b64decode
from typing import Dict, List

from google.protobuf.json_format import MessageToDict
from grpc.experimental import aio as aiogrpc

import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
from ray.core.generated import gcs_pb2
from ray.core.generated import job_pb2
from ray.core.generated import job_pb2_grpc
import ray.utils

import aiohttp.web
import aioredis
import asyncio

from ray.dashboard.modules.job.job_info import JobInfo
import ray.dashboard.modules.job.job_updater as job_updater

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable
DEBUG = False


class JobResponse:
    def __init__(self, success, message, job_id):
        self.success = success
        self.message = message
        self.job_id = job_id

    def to_dict(self):
        return {
            "success": self.success,
            "message": self.message,
            "jobId": self.job_id,
        }


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

    @routes.post("/jobs")
    async def new_job(self, req):
        job_info = dict(await req.json())
        job_info["id"] = job_id = await job_updater.next_job_id(self._aioredis_client)
        job_info["state"] = "submitted"

        # If there is no agents now, let's reject it.
        if len(self._stubs) == 0:
            log_msg = "Failed to submit job because there is no agent connected to job master."
            logger.info(log_msg)
            job_response = JobResponse(success=False, message=log_msg, job_id=job_id).to_dict()
            return await dashboard_utils.json_response(job_response)

        # We select the first agent to start driver for this job.
        ip_to_start_driver = list(self._stubs.keys())[0]
        job_info["assigned_node_name"] = ip_to_start_driver

        # Write job info to GCS synchronously.
        await job_updater.submit_job(self._aioredis_client, job_info)

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
        job_response = JobResponse(success=True, message="Succeeded to submit job.", job_id=job_id).to_dict()
        return await dashboard_utils.json_response(job_response)

    @routes.get("/jobs")
    async def job_list(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        view = req.query.get("view")
        if view == "summary":
            D = await self._get_job_list()
            return await dashboard_utils.json_response(result=D, ts=now)
        else:
            return aiohttp.web.Response(status=403, text="403 Forbidden")

    async def _get_job_list(self):
        all_job_info = await job_updater.get_all_job_info(self._aioredis_client)
        return list(all_job_info.values())

    @routes.get("/jobs/{job_id}")
    async def node_detail(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        job_id = req.match_info.get("job_id")
        view = req.query.get("view")
        if view == "status":
            return await dashboard_utils.json_response(JobResponse(success=True, message="", job_id=job_id).to_dict())
        D = await self._get_job_detail(job_id)
        return await dashboard_utils.json_response(result=D, ts=now)

    async def _get_job_detail(self, job_id):
        job_info = await job_updater.get_job(self._aioredis_client, job_id)
        job_actors = []
        raylet_stats = copy.deepcopy(datacenter.raylet_stats)
        all_actors = self._get_actor_tree2(raylet_stats, flat=True)
        for actor_info in all_actors.values():
            if actor_info["jobId"] == job_id:
                job_actors.append(actor_info)
        job_workers = []
        for hostname, node_stats in copy.deepcopy(datacenter.node_stats).items():
            pid_to_worker_stats = {}
            stats = copy.deepcopy(raylet_stats.get(node_stats["ip"]))
            job_worker_pid = set()
            for worker_stats in stats["workersStats"]:
                d = pid_to_worker_stats.setdefault(worker_stats["pid"], {}).setdefault(
                        worker_stats["workerId"], worker_stats["coreWorkerStats"])
                d["workerId"] = worker_stats["workerId"]
                worker_job_id = d["jobId"]
                if worker_job_id == job_id:
                    job_worker_pid.add(worker_stats["pid"])
            for worker in node_stats["workers"]:
                worker_stats = pid_to_worker_stats.get(worker["pid"], {})
                worker["coreWorkerStats"] = list(worker_stats.values())
                if worker["pid"] in job_worker_pid:
                    job_workers.append(worker)

        return {
            "jobInfo": job_info,
            "jobActors": job_actors,
            "jobWorkers": job_workers,
        }

    # TODO(fyrestone): collect actor info to datacenter
    @staticmethod
    def _get_actor_tree(workers_info_by_node, infeasible_tasks,
                        ready_tasks, flat=False) -> Dict:
        now = time.time()

        # default info
        default_info = {
            "actorId": "",
            "children": {},
            "currentTaskFuncDesc": [],
            "ipAddress": "",
            "isDirectCall": False,
            "jobId": "",
            "numExecutedTasks": 0,
            "numLocalObjects": 0,
            "numObjectIdsInScope": 0,
            "port": 0,
            "state": 0,
            "taskQueueLength": 0,
            "usedObjectStoreMemory": 0,
            "usedResources": {},
        }

        # actor relationship
        addr_to_owner_addr = {}
        addr_to_actor_id = {}
        addr_to_extra_info_dict = {}
        for actor_data in datacenter.actors.values():
            addr = (actor_data["Address"]["IPAddress"],
                    str(actor_data["Address"]["Port"]))
            owner_addr = (actor_data["OwnerAddress"]["IPAddress"],
                          str(actor_data["OwnerAddress"]["Port"]))
            addr_to_owner_addr[addr] = owner_addr
            addr_to_actor_id[addr] = actor_data["ActorID"]
            addr_to_extra_info_dict[addr] = {
                "jobId": actor_data["JobID"],
                "state": gcs_pb2.ActorTableData.ActorState.Name(actor_data["State"]),
                "isDirectCall": actor_data["IsDirectCall"],
                "timestamp": actor_data["Timestamp"]
            }

        # construct flattened actor tree
        flattened_tree = {"root": {"children": {}}}
        child_to_parent = {}
        for addr, actor_id in addr_to_actor_id.items():
            flattened_tree[actor_id] = copy.deepcopy(default_info)
            flattened_tree[actor_id].update(
                    addr_to_extra_info_dict[addr])
            parent_id = addr_to_actor_id.get(
                    addr_to_owner_addr[addr], "root")
            child_to_parent[actor_id] = parent_id

        for node_id, workers_info in workers_info_by_node.items():
            for worker_info in workers_info:
                if "coreWorkerStats" in worker_info:
                    core_worker_stats = worker_info["coreWorkerStats"]
                    addr = (core_worker_stats["ipAddress"],
                            str(core_worker_stats["port"]))
                    if addr in addr_to_actor_id:
                        actor_info = flattened_tree[addr_to_actor_id[
                            addr]]
                        dashboard_utils.format_reply_id(core_worker_stats)
                        actor_info.update(core_worker_stats)
                        actor_info["averageTaskExecutionSpeed"] = round(
                                actor_info["numExecutedTasks"] /
                                (now - actor_info["timestamp"] / 1000), 2)
                        actor_info["nodeId"] = node_id
                        actor_info["pid"] = worker_info["pid"]

        def _update_flatten_tree(task, task_spec_type, invalid_state_type):
            actor_id = ray.utils.binary_to_hex(
                    b64decode(task[task_spec_type]["actorId"]))
            caller_addr = (task["callerAddress"]["ipAddress"],
                           str(task["callerAddress"]["port"]))
            caller_id = addr_to_actor_id.get(caller_addr, "root")
            child_to_parent[actor_id] = caller_id
            task["state"] = -1
            task["invalidStateType"] = invalid_state_type
            task["actorTitle"] = task["functionDescriptor"][
                "pythonFunctionDescriptor"]["className"]
            dashboard_utils.format_reply_id(task)
            flattened_tree[actor_id] = task

        for infeasible_task in infeasible_tasks:
            _update_flatten_tree(infeasible_task, "actorCreationTaskSpec",
                                 "infeasibleActor")

        for ready_task in ready_tasks:
            _update_flatten_tree(ready_task, "actorCreationTaskSpec",
                                 "pendingActor")

        # construct actor tree
        actor_tree = flattened_tree
        if flat:
            actor_tree.pop("root")
            return actor_tree
        else:
            for actor_id, parent_id in child_to_parent.items():
                actor_tree[parent_id]["children"][actor_id] = actor_tree[actor_id]
            return actor_tree["root"]["children"]

    # TODO(fyrestone): collect actor info to datacenter
    def _get_actor_tree2(self, raylet_stats, flat=False):
        workers_info_by_node = {
            data["nodeId"]: data.get("workersStats")
            for data in raylet_stats.values()
        }
        infeasible_tasks = sum(
                (data.get("infeasibleTasks", []) for data in raylet_stats.values()), [])
        # ready_tasks are used to render tasks that are not schedulable
        # due to resource limitations.
        # (e.g., Actor requires 2 GPUs but there is only 1 gpu available).
        ready_tasks = sum((data.get("readyTasks", []) for data in raylet_stats.values()),
                          [])
        return self._get_actor_tree(
                workers_info_by_node, infeasible_tasks, ready_tasks, flat=flat)

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
                'driverEntry': 'simple_job',
                'driverArgs': ['arg1', 'arg2'],
                'customConfig': {'k1': 'v1', 'k2': 'v2'},
                'jvmOptions': '-Dabc=123 -Daaa=xxx',
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
