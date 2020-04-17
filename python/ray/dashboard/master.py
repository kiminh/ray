import asyncio
import logging
import inspect
import os
import yaml
import json
import ray.utils as ray_utils
import ray.dashboard.utils as dashboard_utils
import aiohttp.web

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


class DashboardMaster:
    def __init__(self, redis_address, redis_password):
        self.redis_address = redis_address
        self.redis_password = redis_password
        self._modules, self._methods = self._load_modules()
        dashboard_utils.ClassMethodRouteTable.bind(self)

    def _load_modules(self):
        """Load dashboard master modules."""
        agent_cls_list = dashboard_utils.get_all_modules(dashboard_utils.TYPE_MASTER)
        modules = []
        methods = {}
        for cls in agent_cls_list:
            logger.info("Load %s module: %s", dashboard_utils.TYPE_MASTER, cls)
            c = cls(redis_address=self.redis_address,
                    redis_password=self.redis_password)
            dashboard_utils.ClassMethodRouteTable.bind(c)
            modules.append(c)
            module_methods = inspect.getmembers(
                    c, predicate=ray_utils.is_function_or_method)
            for method_name, method in module_methods:
                if method_name in ("__init__", "run"):
                    continue
                if method_name in methods:
                    raise Exception("Conflict method {}".format(method_name))
                else:
                    methods[method_name] = method
        return modules, methods

    def __getattr__(self, item):
        try:
            return self._methods[item]
        except KeyError:
            raise AttributeError("'{}' object has no attribute '{}'".format(
                    type(self).__name__, item))

    def _construct_raylet_info(self):
        D = self.get_raylet_stats()
        workers_info_by_node = {
            data["nodeId"]: data.get("workersStats")
            for data in D.values()
        }
        infeasible_tasks = sum(
                (data.get("infeasibleTasks", []) for data in D.values()), [])
        # ready_tasks are used to render tasks that are not schedulable
        # due to resource limitations.
        # (e.g., Actor requires 2 GPUs but there is only 1 gpu available).
        ready_tasks = sum((data.get("readyTasks", []) for data in D.values()),
                          [])
        actor_tree = self.get_actor_tree(
                workers_info_by_node, infeasible_tasks, ready_tasks)
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
            data["extraInfo"] = ", ".join(extra_info_strings) + "\n"
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

    def get_ray_config(self):
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
        error, result = self.get_ray_config()
        if error:
            return await dashboard_utils.json_response(error=error)
        return await dashboard_utils.json_response(result=result)

    @routes.get("/api/raylet_info")
    async def raylet_info(self, req) -> aiohttp.web.Response:
        result = self._construct_raylet_info()
        return await dashboard_utils.json_response(result=result)

    async def run(self):
        await asyncio.gather(*(m.run() for m in self._modules))
