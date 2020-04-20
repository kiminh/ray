import asyncio
import logging

import aioredis
from grpc.experimental import aio

import ray.dashboard.consts as dashboard_consts
import ray.dashboard.datacenter as datacenter
import ray.dashboard.utils as dashboard_utils
import ray.services

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable

aio.init_grpc_aio()


class DashboardMaster:
    def __init__(self, redis_address, redis_password):
        self.redis_address = redis_address
        self.redis_password = redis_password
        self.redis_client = ray.services.create_redis_client(
                redis_address, password=redis_password)
        ray.state.state._initialize_global_state(
                redis_address=redis_address,
                redis_password=redis_password)
        self._modules = self._load_modules()

    async def create_aioredis_client(self):
        ip, port = self.redis_address.split(":")
        return await aioredis.create_redis(
                (ip, int(port)), password=self.redis_password)

    async def _update_agents(self):
        aioredis_client = await self.create_aioredis_client()

        while True:
            try:
                nodes = ray.nodes()
                node_ips = [node["NodeManagerAddress"] for node in nodes]

                # First remove node connections of disconnected nodes.
                for node_ip in datacenter.agents:
                    if node_ip not in node_ips:
                        datacenter.agents.pop(node_ip)

                # Now add node connections of new nodes.
                for node in nodes:
                    node_ip = node["NodeManagerAddress"]
                    if node_ip not in datacenter.agents:
                        key = "{}{}".format(
                                dashboard_consts.DASHBOARD_AGENT_PORT_PREFIX,
                                node_ip)
                        agent_port = await aioredis_client.get(key)
                        if agent_port:
                            datacenter.agents[node_ip] = agent_port
            finally:
                await asyncio.sleep(2.0)

    def _load_modules(self):
        """Load dashboard master modules."""
        agent_cls_list = dashboard_utils.get_all_modules(dashboard_consts.TYPE_MASTER)
        modules = []
        for cls in agent_cls_list:
            logger.info("Load %s module: %s", dashboard_consts.TYPE_MASTER, cls)
            c = cls(self)
            dashboard_utils.ClassMethodRouteTable.bind(c)
            modules.append(c)
        return modules

    async def run(self):
        async def _async_notify():
            while True:
                co = await datacenter.NotifyQueue.get()
                await co

        datacenter.NotifyQueue.freeze_signal()
        await asyncio.gather(self._update_agents(), _async_notify(), *(m.run() for m in self._modules))
