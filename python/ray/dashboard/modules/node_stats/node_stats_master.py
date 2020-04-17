import copy
import datetime
import json
import logging
import re
import time
import traceback
from base64 import b64decode
from collections import defaultdict
from operator import itemgetter
from typing import Dict

import aiohttp.web
import aioredis
from aioredis.pubsub import Receiver

import ray
import ray.gcs_utils
import ray.dashboard.utils as dashboard_utils
import ray.services
import ray.utils

logger = logging.getLogger(__name__)
routes = aiohttp.web.RouteTableDef()


@dashboard_utils.master
class NodeStats:
    def __init__(self, redis_address, redis_password=None):
        self.redis_key = "{}.*".format(ray.gcs_utils.REPORTER_CHANNEL)
        ip, port = redis_address.split(":")
        self.redis_client = aioredis.create_redis(
                (ip, int(port)), password=redis_password)

        self._node_stats = {}
        self._addr_to_owner_addr = {}
        self._addr_to_actor_id = {}
        self._addr_to_extra_info_dict = {}

        self._default_info = {
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

        # Mapping from IP address to PID to list of log lines
        self._logs = defaultdict(lambda: defaultdict(list))

        # Mapping from IP address to PID to list of error messages
        self._errors = defaultdict(lambda: defaultdict(list))

        ray.state.state._initialize_global_state(
                redis_address=redis_address, redis_password=redis_password)

        super().__init__()

    def _calculate_log_counts(self):
        return {
            ip: {
                pid: len(logs_for_pid)
                for pid, logs_for_pid in logs_for_ip.items()
            }
            for ip, logs_for_ip in self._logs.items()
        }

    def _calculate_error_counts(self):
        return {
            ip: {
                pid: len(errors_for_pid)
                for pid, errors_for_pid in errors_for_ip.items()
            }
            for ip, errors_for_ip in self._errors.items()
        }

    def _purge_outdated_stats(self):
        def current(then, now):
            if (now - then) > 5:
                return False

            return True

        now = dashboard_utils.to_unix_time(datetime.datetime.utcnow())
        self._node_stats = {
            k: v
            for k, v in self._node_stats.items() if current(v["now"], now)
        }

    @routes.get("/api/node_info")
    async def node_info(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        D = self.get_node_stats()
        return await dashboard_utils.json_response(result=D, ts=now)

    def get_node_stats(self) -> Dict:
        self._purge_outdated_stats()
        node_stats = sorted(
                (v for v in self._node_stats.values()),
                key=itemgetter("boot_time"))
        return {
            "clients": node_stats,
            "log_counts": self._calculate_log_counts(),
            "error_counts": self._calculate_error_counts(),
        }

    def get_actor_tree(self, workers_info_by_node, infeasible_tasks,
                       ready_tasks) -> Dict:
        now = time.time()
        # construct flattened actor tree
        flattened_tree = {"root": {"children": {}}}
        child_to_parent = {}
        for addr, actor_id in self._addr_to_actor_id.items():
            flattened_tree[actor_id] = copy.deepcopy(self._default_info)
            flattened_tree[actor_id].update(
                    self._addr_to_extra_info_dict[addr])
            parent_id = self._addr_to_actor_id.get(
                    self._addr_to_owner_addr[addr], "root")
            child_to_parent[actor_id] = parent_id

        for node_id, workers_info in workers_info_by_node.items():
            for worker_info in workers_info:
                if "coreWorkerStats" in worker_info:
                    core_worker_stats = worker_info["coreWorkerStats"]
                    addr = (core_worker_stats["ipAddress"],
                            str(core_worker_stats["port"]))
                    if addr in self._addr_to_actor_id:
                        actor_info = flattened_tree[self._addr_to_actor_id[
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
            caller_id = self._addr_to_actor_id.get(caller_addr, "root")
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
        for actor_id, parent_id in child_to_parent.items():
            actor_tree[parent_id]["children"][actor_id] = actor_tree[actor_id]
        return actor_tree["root"]["children"]

    @routes.get("/api/logs")
    async def logs(self, req) -> aiohttp.web.Response:
        hostname = req.query.get("hostname")
        pid = req.query.get("pid")
        result = self.get_logs(hostname, pid)
        return await dashboard_utils.json_response(result=result)

    def get_logs(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        logs = self._logs.get(ip, {})
        if pid:
            logs = {pid: logs.get(pid, [])}
        return logs

    @routes.get("/api/errors")
    async def errors(self, req) -> aiohttp.web.Response:
        hostname = req.query.get("hostname")
        pid = req.query.get("pid")
        result = self.get_errors(hostname, pid)
        return await dashboard_utils.json_response(result=result)

    def get_errors(self, hostname, pid):
        ip = self._node_stats.get(hostname, {"ip": None})["ip"]
        errors = self._errors.get(ip, {})
        if pid:
            errors = {pid: errors.get(pid, [])}
        return errors

    async def run(self):
        p = await self.redis_client
        mpsc = Receiver()

        await p.psubscribe(mpsc.pattern(self.redis_key))
        logger.info("NodeStats: subscribed to {}".format(self.redis_key))

        log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
        await p.subscribe(mpsc.channel(log_channel))
        logger.info("NodeStats: subscribed to {}".format(log_channel))

        error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        await p.subscribe(mpsc.channel(error_channel))
        logger.info("NodeStats: subscribed to {}".format(error_channel))

        actor_channel = ray.gcs_utils.TablePubsub.Value("ACTOR_PUBSUB")
        await p.subscribe(mpsc.channel(actor_channel))
        logger.info("NodeStats: subscribed to {}".format(actor_channel))

        current_actor_table = ray.actors()
        for actor_data in current_actor_table.values():
            addr = (actor_data["Address"]["IPAddress"],
                    str(actor_data["Address"]["Port"]))
            owner_addr = (actor_data["OwnerAddress"]["IPAddress"],
                          str(actor_data["OwnerAddress"]["Port"]))
            self._addr_to_owner_addr[addr] = owner_addr
            self._addr_to_actor_id[addr] = actor_data["ActorID"]
            self._addr_to_extra_info_dict[addr] = {
                "jobId": actor_data["JobID"],
                "state": actor_data["State"],
                "isDirectCall": actor_data["IsDirectCall"],
                "timestamp": actor_data["Timestamp"]
            }

        async for sender, msg in mpsc.iter():
            try:
                channel = ray.utils.decode(sender.name)
                _, data = msg
                if channel == log_channel:
                    data = json.loads(ray.utils.decode(data))
                    ip = data["ip"]
                    pid = str(data["pid"])
                    self._logs[ip][pid].extend(data["lines"])
                elif channel == str(error_channel):
                    gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                    error_data = ray.gcs_utils.ErrorTableData.FromString(
                            gcs_entry.entries[0])
                    message = error_data.error_message
                    message = re.sub(r"\x1b\[\d+m", "", message)
                    match = re.search(r"\(pid=(\d+), ip=(.*?)\)", message)
                    if match:
                        pid = match.group(1)
                        ip = match.group(2)
                        self._errors[ip][pid].append({
                            "message": message,
                            "timestamp": error_data.timestamp,
                            "type": error_data.type
                        })
                elif channel == str(actor_channel):
                    gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                    actor_data = ray.gcs_utils.ActorTableData.FromString(
                            gcs_entry.entries[0])
                    addr = (actor_data.address.ip_address,
                            str(actor_data.address.port))
                    owner_addr = (actor_data.owner_address.ip_address,
                                  str(actor_data.owner_address.port))
                    self._addr_to_owner_addr[addr] = owner_addr
                    self._addr_to_actor_id[addr] = ray.utils.binary_to_hex(
                            actor_data.actor_id)
                    self._addr_to_extra_info_dict[addr] = {
                        "jobId": ray.utils.binary_to_hex(
                                actor_data.job_id),
                        "state": actor_data.state,
                        "isDirectCall": True,
                        "timestamp": actor_data.timestamp
                    }
                else:
                    data = json.loads(ray.utils.decode(data))
                    self._node_stats[data["hostname"]] = data

            except Exception:
                logger.exception(traceback.format_exc())
