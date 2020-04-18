import asyncio
import logging

import aioredis

import ray.dashboard.consts as dashboard_consts
import ray.dashboard.utils as dashboard_utils
import ray.services

logger = logging.getLogger(__name__)
routes = dashboard_utils.ClassMethodRouteTable


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
        dashboard_utils.ClassMethodRouteTable.bind(self)

    async def create_aioredis_client(self):
        ip, port = self.redis_address.split(":")
        return await aioredis.create_redis(
                (ip, int(port)), password=self.redis_password)

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
        await asyncio.gather(*(m.run() for m in self._modules))
