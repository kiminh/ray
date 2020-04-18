import asyncio
import datetime
import json
import logging
import traceback
import uuid
from operator import itemgetter
from typing import Dict

import aiohttp.web
import grpc
from aioredis.pubsub import Receiver

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
import ray.gcs_utils
import ray.services
import ray.utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.master
class ReportMaster:
    def __init__(self, dashboard_master):
        self.dashboard_master = dashboard_master
        self.reporter_stubs = {}
        self._profiling_stats = {}
        self._update_stubs()

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
                reporter_port = dashboard_utils.get_agent_port(self.dashboard_master.redis_client, node_ip)
                if not reporter_port:
                    continue
                reporter_channel = grpc.insecure_channel("{}:{}".format(
                        node_ip, int(reporter_port)))
                reporter_stub = reporter_pb2_grpc.ReporterServiceStub(
                        reporter_channel)
                self.reporter_stubs[node_id] = reporter_stub

    @routes.get("/api/node_info")
    async def node_info(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        D = self._get_node_info()
        return await dashboard_utils.json_response(result=D, ts=now)

    @staticmethod
    def _purge_outdated_stats():
        def current(then, now):
            if (now - then) > 5:
                return False

            return True

        now = dashboard_utils.to_unix_time(datetime.datetime.utcnow())
        datacenter.node_stats = {
            k: v
            for k, v in datacenter.node_stats.items() if current(v["now"], now)
        }

    @staticmethod
    def _calculate_log_counts():
        return {
            ip: {
                pid: len(logs_for_pid)
                for pid, logs_for_pid in logs_for_ip.items()
            }
            for ip, logs_for_ip in datacenter.logs.items()
        }

    @staticmethod
    def _calculate_error_counts():
        return {
            ip: {
                pid: len(errors_for_pid)
                for pid, errors_for_pid in errors_for_ip.items()
            }
            for ip, errors_for_ip in datacenter.errors.items()
        }

    def _get_node_info(self) -> Dict:
        self._purge_outdated_stats()
        node_stats = sorted(
                (v for v in datacenter.node_stats.values()),
                key=itemgetter("boot_time"))
        return {
            "clients": node_stats,
            "log_counts": self._calculate_log_counts(),
            "error_counts": self._calculate_error_counts(),
        }

    @routes.get("/api/launch_profiling")
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        node_id = req.query.get("node_id")
        pid = int(req.query.get("pid"))
        duration = int(req.query.get("duration"))
        profiling_id = self._launch_profiling(
                node_id, pid, duration)
        return await dashboard_utils.json_response(result=str(profiling_id))

    def _launch_profiling(self, node_id, pid, duration):
        profiling_id = str(uuid.uuid4())

        def _callback(reply_future):
            reply = reply_future.result()
            self._profiling_stats[profiling_id] = reply

        reporter_stub = self.reporter_stubs[node_id]
        reply_future = reporter_stub.GetProfilingStats.future(
                reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        reply_future.add_done_callback(_callback)
        return profiling_id

    @routes.get("/api/check_profiling_status")
    async def check_profiling_status(self, req) -> aiohttp.web.Response:
        profiling_id = req.query.get("profiling_id")
        status = self._check_profiling_status(profiling_id)
        return await dashboard_utils.json_response(result=status)

    def _check_profiling_status(self, profiling_id):
        is_present = profiling_id in self._profiling_stats
        if not is_present:
            return {"status": "pending"}

        reply = self._profiling_stats[profiling_id]
        if reply.stderr:
            return {"status": "error", "error": reply.stderr}
        else:
            return {"status": "finished"}

    @routes.get("/api/get_profiling_info")
    async def get_profiling_info(self, req) -> aiohttp.web.Response:
        profiling_id = req.query.get("profiling_id")
        profiling_info = self._get_profiling_info(
                profiling_id)
        return aiohttp.web.json_response(profiling_info)

    def _get_profiling_info(self, profiling_id):
        profiling_stats = self._profiling_stats.get(profiling_id)
        assert profiling_stats, "profiling not finished"
        return json.loads(profiling_stats.profiling_stats)

    async def run(self):
        p = await self.dashboard_master.create_aioredis_client()
        mpsc = Receiver()

        reporter_key = "{}*".format(dashboard_consts.REPORTER_PREFIX)
        await p.psubscribe(mpsc.pattern(reporter_key))
        logger.info("subscribed to {}".format(reporter_key))

        async def _update_node_stats():
            async for sender, msg in mpsc.iter():
                try:
                    channel = ray.utils.decode(sender.name)
                    _, data = msg
                    data = json.loads(ray.utils.decode(data))
                    datacenter.node_stats[data["hostname"]] = data
                except Exception:
                    logger.exception(traceback.format_exc())

        async def _update_stubs():
            while True:
                self._update_stubs()
                await asyncio.sleep(10.0)

        await asyncio.gather(_update_node_stats(), _update_stubs())
