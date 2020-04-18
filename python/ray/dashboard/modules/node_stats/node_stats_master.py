import json
import logging
import re
import traceback

import aiohttp.web
from aioredis.pubsub import Receiver

import ray
import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
import ray.gcs_utils
import ray.services
import ray.utils

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


@dashboard_utils.master
class NodeStats:
    def __init__(self, dashboard_master):
        self.dashboard_master = dashboard_master

    @routes.get("/api/logs")
    async def logs(self, req) -> aiohttp.web.Response:
        hostname = req.query.get("hostname")
        pid = req.query.get("pid")
        result = self._get_logs(hostname, pid)
        return await dashboard_utils.json_response(result=result)

    @staticmethod
    def _get_logs(hostname, pid):
        ip = datacenter.node_stats.get(hostname, {"ip": None})["ip"]
        logs = datacenter.logs.get(ip, {})
        if pid:
            logs = {pid: logs.get(pid, [])}
        return logs

    @routes.get("/api/errors")
    async def errors(self, req) -> aiohttp.web.Response:
        hostname = req.query.get("hostname")
        pid = req.query.get("pid")
        result = self._get_errors(hostname, pid)
        return await dashboard_utils.json_response(result=result)

    @staticmethod
    def _get_errors(hostname, pid):
        ip = datacenter.node_stats.get(hostname, {"ip": None})["ip"]
        errors = datacenter.errors.get(ip, {})
        if pid:
            errors = {pid: errors.get(pid, [])}
        return errors

    async def run(self):
        p = await self.dashboard_master.create_aioredis_client()
        mpsc = Receiver()

        log_channel = ray.gcs_utils.LOG_FILE_CHANNEL
        await p.subscribe(mpsc.channel(log_channel))
        logger.info("subscribed to {}".format(log_channel))

        error_channel = ray.gcs_utils.TablePubsub.Value("ERROR_INFO_PUBSUB")
        await p.subscribe(mpsc.channel(error_channel))
        logger.info("subscribed to {}".format(error_channel))

        actor_channel = ray.gcs_utils.TablePubsub.Value("ACTOR_PUBSUB")
        await p.subscribe(mpsc.channel(actor_channel))
        logger.info("subscribed to {}".format(actor_channel))

        current_actor_table = ray.actors()
        for actor_data in current_actor_table.values():
            datacenter.actors[actor_data["ActorID"]] = actor_data

        async for sender, msg in mpsc.iter():
            try:
                channel = ray.utils.decode(sender.name)
                _, data = msg
                if channel == log_channel:
                    data = json.loads(ray.utils.decode(data))
                    ip = data["ip"]
                    pid = str(data["pid"])
                    datacenter.logs[ip][pid].extend(data["lines"])
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
                        datacenter.errors[ip][pid].append({
                            "message": message,
                            "timestamp": error_data.timestamp,
                            "type": error_data.type
                        })
                elif channel == str(actor_channel):
                    gcs_entry = ray.gcs_utils.GcsEntry.FromString(data)
                    actor_data = ray.gcs_utils.ActorTableData.FromString(
                            gcs_entry.entries[0])
                    datacenter.actors[actor_data["ActorID"]] = actor_data

            except Exception:
                logger.exception(traceback.format_exc())
