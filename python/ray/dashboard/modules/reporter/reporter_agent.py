import asyncio
import datetime
import json
import logging
import os
import socket
import subprocess
import sys
import traceback

import aioredis
import psutil

import ray
import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.gcs_utils
import ray.services
import ray.utils
from ray.core.generated import reporter_pb2
from ray.core.generated import reporter_pb2_grpc

logger = logging.getLogger(__name__)


class ReporterServer(reporter_pb2_grpc.ReporterServiceServicer):
    def __init__(self):
        pass

    async def GetProfilingStats(self, request, context):
        pid = request.pid
        duration = request.duration
        profiling_file_path = os.path.join(ray.utils.get_ray_temp_dir(),
                                           "{}_profiling.txt".format(pid))
        process = subprocess.Popen(
                "sudo $(which py-spy) record -o {} -p {} -d {} -f speedscope"
                    .format(profiling_file_path, pid, duration),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                shell=True)
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            profiling_stats = ""
        else:
            with open(profiling_file_path, "r") as f:
                profiling_stats = f.read()
        return reporter_pb2.GetProfilingStatsReply(
                profiling_stats=profiling_stats, stdout=stdout, stderr=stderr)


def recursive_asdict(o):
    if isinstance(o, tuple) and hasattr(o, "_asdict"):
        return recursive_asdict(o._asdict())

    if isinstance(o, (tuple, list)):
        L = []
        for k in o:
            L.append(recursive_asdict(k))
        return L

    if isinstance(o, dict):
        D = {k: recursive_asdict(v) for k, v in o.items()}
        return D

    return o


def jsonify_asdict(o):
    return json.dumps(recursive_asdict(o))


def is_worker(cmdline):
    return cmdline and cmdline[0].startswith("ray::")


def to_posix_time(dt):
    return (dt - datetime.datetime(1970, 1, 1)).total_seconds()


@dashboard_utils.agent
class Reporter:
    """A monitor process for monitoring Ray nodes.

    Attributes:
        dashboard_agent: The DashboardAgent object contains global config
    """

    def __init__(self, dashboard_agent):
        """Initialize the reporter object."""
        self.dashboard_agent = dashboard_agent
        self.cpu_counts = (psutil.cpu_count(), psutil.cpu_count(logical=False))
        self.ip = ray.services.get_node_ip_address()
        self.hostname = socket.gethostname()

        _ = psutil.cpu_percent()  # For initialization

        self.network_stats_hist = [(0, (0.0, 0.0))]  # time, (sent, recv)

    @staticmethod
    def get_cpu_percent():
        return psutil.cpu_percent()

    @staticmethod
    def get_boot_time():
        return psutil.boot_time()

    @staticmethod
    def get_network_stats():
        ifaces = [
            v for k, v in psutil.net_io_counters(pernic=True).items()
            if k[0] == "e"
        ]

        sent = sum((iface.bytes_sent for iface in ifaces))
        recv = sum((iface.bytes_recv for iface in ifaces))
        return sent, recv

    @staticmethod
    def get_mem_usage():
        vm = psutil.virtual_memory()
        return vm.total, vm.available, vm.percent

    @staticmethod
    def get_disk_usage():
        dirs = [
            os.environ["USERPROFILE"] if sys.platform == "win32" else os.sep,
            ray.utils.get_user_temp_dir(),
        ]
        return {x: psutil.disk_usage(x) for x in dirs}

    @staticmethod
    def get_workers():
        return [
            x.as_dict(attrs=[
                "pid",
                "create_time",
                "cpu_percent",
                "cpu_times",
                "cmdline",
                "memory_info",
            ]) for x in psutil.process_iter(attrs=["cmdline"])
            if is_worker(x.info["cmdline"])
        ]

    def get_load_avg(self):
        if sys.platform == "win32":
            cpu_percent = psutil.cpu_percent()
            load = (cpu_percent, cpu_percent, cpu_percent)
        else:
            load = os.getloadavg()
        per_cpu_load = tuple((round(x / self.cpu_counts[0], 2) for x in load))
        return load, per_cpu_load

    def get_all_stats(self):
        now = to_posix_time(datetime.datetime.utcnow())
        network_stats = self.get_network_stats()

        self.network_stats_hist.append((now, network_stats))
        self.network_stats_hist = self.network_stats_hist[-7:]
        then, prev_network_stats = self.network_stats_hist[0]
        netstats = ((network_stats[0] - prev_network_stats[0]) / (now - then),
                    (network_stats[1] - prev_network_stats[1]) / (now - then))

        return {
            "now": now,
            "hostname": self.hostname,
            "ip": self.ip,
            "cpu": self.get_cpu_percent(),
            "cpus": self.cpu_counts,
            "mem": self.get_mem_usage(),
            "workers": self.get_workers(),
            "boot_time": self.get_boot_time(),
            "load_avg": self.get_load_avg(),
            "disk": self.get_disk_usage(),
            "net": netstats,
        }

    async def perform_iteration(self):
        """Get any changes to the log files and push updates to Redis."""
        aioredis_client = await aioredis.create_redis(
                address=self.dashboard_agent.redis_address,
                password=self.dashboard_agent.redis_password)

        while True:
            try:
                stats = self.get_all_stats()
                await aioredis_client.publish(
                        "{}{}".format(dashboard_consts.REPORTER_PREFIX, self.hostname),
                        jsonify_asdict(stats))
            except Exception:
                traceback.print_exc()
            await asyncio.sleep(dashboard_consts.REPORTER_UPDATE_INTERVAL_MS / 1000)

    async def run(self, server):
        """Publish the port."""
        reporter_pb2_grpc.add_ReporterServiceServicer_to_server(
                ReporterServer(), server)
        await self.perform_iteration()
