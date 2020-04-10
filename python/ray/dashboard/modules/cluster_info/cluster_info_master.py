import copy
import json
import logging
import os
import time
import datetime
from base64 import b64decode
from typing import Dict, List
from operator import itemgetter

import aiohttp.web
import yaml

import ray
import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
import ray.services
import ray.utils
from ray.core.generated import gcs_pb2

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.master
class ClusterInfo:
    def __init__(self, dashboard_master):
        self.dashboard_master = dashboard_master

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

    def _construct_raylet_info(self):
        D = copy.deepcopy(datacenter.raylet_stats)
        actor_tree = self._get_actor_tree2(D)
        for address, data in D.items():
            # process view data
            measures_dicts = {}
            for view_data in data["viewData"]:
                view_name = view_data["viewName"]
                if view_name in ("local_available_resource",
                                 "local_total_resource",
                                 "object_manager_stats"):
                    measures_dicts[view_name] = dashboard_utils.measures_to_dict(
                            view_data["measures"])
            # process resources info
            extra_info_strings = []
            prefix = "ResourceName:"
            for resource_name, total_resource in measures_dicts[
                "local_total_resource"].items():
                available_resource = measures_dicts[
                    "local_available_resource"].get(resource_name, .0)
                resource_name = resource_name[len(prefix):]
                extra_info_strings.append("{}: {} / {}".format(
                        resource_name,
                        dashboard_utils.format_resource(resource_name,
                                                        total_resource - available_resource),
                        dashboard_utils.format_resource(resource_name, total_resource)))
            data["extraInfo"] = ", ".join(extra_info_strings)
            if os.environ.get("RAY_DASHBOARD_DEBUG"):
                # process object store info
                extra_info_strings = []
                prefix = "ValueType:"
                for stats_name in [
                    "used_object_store_memory", "num_local_objects"
                ]:
                    stats_value = measures_dicts["object_manager_stats"].get(
                            prefix + stats_name, .0)
                    extra_info_strings.append("{}: {}".format(
                            stats_name, stats_value))
                data["extraInfo"] += ", ".join(extra_info_strings)
                # process actor info
                actor_tree_str = json.dumps(
                        actor_tree, indent=2, sort_keys=True)
                lines = actor_tree_str.split("\n")
                max_line_length = max(map(len, lines))
                to_print = []
                for line in lines:
                    to_print.append(line + (max_line_length - len(line)) * " ")
                data["extraInfo"] += "\n" + "\n".join(to_print)
        return {"nodes": D, "actors": actor_tree}

    @staticmethod
    def _get_node_list2() -> List:
        datacenter.purge_outdated_stats()
        node_stats = sorted(
                (copy.deepcopy(v) for v in datacenter.node_stats.values()),
                key=itemgetter("bootTime"))
        log_counts = datacenter.calculate_log_counts()
        error_counts = datacenter.calculate_error_counts()
        for node_stat in node_stats:
            node_stat["logCounts"] = sum(count for count in log_counts.get(node_stat["ip"], {}).values())
            node_stat["errorCounts"] = sum(count for count in error_counts.get(node_stat["ip"], {}).values())
        return node_stats

    @routes.get("/nodes")
    async def node_list(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        view = req.query.get("view")
        if view == "summary":
            D = self._get_node_list()
            return await dashboard_utils.json_response(result=D, ts=now)
        else:
            return aiohttp.web.Response(status=403, text="403 Forbidden")

    def _get_node_list(self):
        node_list = self._get_node_list2()
        raylet_stats = copy.deepcopy(datacenter.raylet_stats)
        for node in node_list:
            node_ip = node["ip"]
            raylet_info = raylet_stats.get(node_ip, {})
            raylet_info.pop("workersStats", None)
            raylet_info.pop("viewData", None)
            node.pop("workers", None)
            node["raylet"] = raylet_info
        return node_list

    @routes.get("/nodes/{hostname}")
    async def node_detail(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        hostname = req.match_info.get("hostname")
        D = self._get_node_detail(hostname)
        return await dashboard_utils.json_response(result=D, ts=now)

    def _get_node_detail(self, hostname):
        node_list = self._get_node_list2()
        raylet_stats = copy.deepcopy(datacenter.raylet_stats)
        actors = self._get_actor_tree2(raylet_stats, flat=True)
        for node in node_list:
            if node["hostname"] != hostname:
                continue
            node_ip = node["ip"]
            raylet_info = raylet_stats.get(node_ip, {})
            node["raylet"] = raylet_info
            # merge worker stats to worker info
            workers_stats = raylet_info.pop("workersStats", {})
            pid_to_worker_stats = {}
            for stats in workers_stats:
                d = pid_to_worker_stats.setdefault(stats["pid"], {}).setdefault(
                        stats["workerId"], stats["coreWorkerStats"])
                d["workerId"] = stats["workerId"]
            for worker in node["workers"]:
                worker_stats = pid_to_worker_stats.get(worker["pid"], {})
                worker["coreWorkerStats"] = list(worker_stats.values())
            # filter actors by node id
            node["actors"] = dict((actor_id, actor)
                                  for actor_id, actor in actors.items()
                                  if actor.get("nodeId") == raylet_info["nodeId"])
            return node
        return {}

    @staticmethod
    def _get_ray_config():
        try:
            config_path = os.path.expanduser("~/ray_bootstrap_config.yaml")
            with open(config_path) as f:
                cfg = yaml.safe_load(f)
        except Exception:
            error = "No config"
            return error, None

        D = {
            "min_workers": cfg["min_workers"],
            "max_workers": cfg["max_workers"],
            "initial_workers": cfg["initial_workers"],
            "autoscaling_mode": cfg["autoscaling_mode"],
            "idle_timeout_minutes": cfg["idle_timeout_minutes"],
        }

        try:
            D["head_type"] = cfg["head_node"]["InstanceType"]
        except KeyError:
            D["head_type"] = "unknown"

        try:
            D["worker_type"] = cfg["worker_nodes"]["InstanceType"]
        except KeyError:
            D["worker_type"] = "unknown"

        return None, D

    @routes.get("/api/ray_config")
    async def ray_config(self, req) -> aiohttp.web.Response:
        error, result = self._get_ray_config()
        if error:
            return await dashboard_utils.json_response(error=error)
        return await dashboard_utils.json_response(result=result)

    @routes.get("/api/raylet_info")
    async def raylet_info(self, req) -> aiohttp.web.Response:
        result = self._construct_raylet_info()
        return await dashboard_utils.json_response(result=result)

    async def run(self):
        pass
