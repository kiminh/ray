import aiohttp
import asyncio
import pytest
import aioredis

import ray.dashboard.modules.job.job_updater as job_updater
import ray


def test_next_job_id():
    ray.init()
    redis_address = ray._get_runtime_context().redis_address
    aioredis_client = await aioredis.create_redis(
        address=redis_address)
    assert ray.JobID.from_int(2) == job_updater.next_job_id(aioredis_client)


def test_update_job_state():
    pass
