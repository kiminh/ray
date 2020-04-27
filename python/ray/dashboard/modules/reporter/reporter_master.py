import datetime
import json
import logging
import traceback
import uuid
from operator import itemgetter
from typing import Dict

import aiohttp.web
import aioredis
from aioredis.pubsub import Receiver
from grpc.experimental import aio as aiogrpc

import ray
import ray.dashboard.datacenter as datacenter
import ray.dashboard.modules.reporter.reporter_consts as reporter_consts
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
        self.stubs = {}
        self._profiling_stats = {}
        datacenter.agents.signal.append(self._update_stubs)

    async def _update_stubs(self, change):
        if change.new:
            ip, port = next(iter(change.new.items()))
            channel = aiogrpc.insecure_channel("{}:{}".format(
                    ip, int(port)))
            stub = reporter_pb2_grpc.ReporterServiceStub(
                    channel)
            self.stubs[ip] = stub
        if change.old:
            ip, port = next(iter(change.new.items()))
            stub = self.stubs.pop(ip)
            stub.close()

    @routes.get("/api/node_info")
    async def node_info(self, req) -> aiohttp.web.Response:
        now = datetime.datetime.utcnow()
        D = self._get_node_info()
        return await dashboard_utils.json_response(result=D, ts=now)

    def _get_node_info(self) -> Dict:
        datacenter.purge_outdated_stats()
        node_stats = sorted(
                (v for v in datacenter.node_stats.values()),
                key=itemgetter("boot_time"))
        return {
            "clients": node_stats,
            "log_counts": datacenter.calculate_log_counts(),
            "error_counts": datacenter.calculate_error_counts(),
        }

    @routes.get("/api/launch_profiling")
    async def launch_profiling(self, req) -> aiohttp.web.Response:
        node_id = req.query.get("node_id")
        pid = int(req.query.get("pid"))
        duration = int(req.query.get("duration"))
        profiling_id = await self._launch_profiling(
                node_id, pid, duration)
        return await dashboard_utils.json_response(result=str(profiling_id))

    async def _launch_profiling(self, node_id, pid, duration):
        profiling_id = str(uuid.uuid4())
        reporter_stub = self.stubs[node_id]
        reply = await reporter_stub.GetProfilingStats(
                reporter_pb2.GetProfilingStatsRequest(pid=pid, duration=duration))
        self._profiling_stats[profiling_id] = reply
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
        p = await aioredis.create_redis(
                address=self.dashboard_master.redis_address,
                password=self.dashboard_master.redis_password)
        mpsc = Receiver()

        reporter_key = "{}*".format(reporter_consts.REPORTER_PREFIX)
        await p.psubscribe(mpsc.pattern(reporter_key))
        logger.info("subscribed to {}".format(reporter_key))

        async for sender, msg in mpsc.iter():
            try:
                channel = ray.utils.decode(sender.name)
                _, data = msg
                data = json.loads(ray.utils.decode(data))
                datacenter.node_stats[data["hostname"]] = data
            except Exception:
                logger.exception(traceback.format_exc())