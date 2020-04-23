import copy
import datetime
import json

import ray.dashboard.modules.job.job_consts as job_consts
import ray

class JobStatusKey:
    State = "state"
    DriverStarted = "driver_started"
    SubmitTime = "submit_time"
    StartTime = "start_time"
    EndTime = "end_time"


class JobState:
    Submitted = "submitted"
    Dispatched = "dispatched"


async def next_job_id(aioredis_client):
    counter_str = await aioredis_client.incr("JobCounter")
    return ray.JobID.from_int(int(counter_str))


async def submit_job(aioredis_client, job_info):
    job_info = copy.deepcopy(job_info)
    job_state = {
        JobStatusKey.State: JobState.Submitted,
        JobStatusKey.DriverStarted: False,
        JobStatusKey.SubmitTime: datetime.datetime.now().isoformat(),
        JobStatusKey.StartTime: None,
        JobStatusKey.EndTime: None,
    }
    for key in job_state.keys():
        value = job_info.pop(key, None)
        if value is not None:
            job_state[key] = value
    await aioredis_client.hset(job_consts.JOB_INFO_TABLE_NAME,
                               job_info["id"],
                               json.dumps(job_info))
    for key, value in job_state.items():
        await aioredis_client.hset(job_consts.JOB_STATUS_TABLE_NAME,
                                   job_info["id"] + ":" + key,
                                   json.dumps(value))


async def get_job(aioredis_client, job_id):
    job_info = await aioredis_client.hget(job_consts.JOB_INFO_TABLE_NAME, job_id)
    job_info = json.loads(job_info)
    for name, key in vars(JobStatusKey).items():
        if not name.startswith("_"):
            value = await aioredis_client.hget(job_consts.JOB_STATUS_TABLE_NAME, job_id + ":" + key)
            job_info[key] = json.loads(value)
    return job_info


async def get_all_job_ids(aioredis_client):
    return await aioredis_client.hgetall(job_consts.JOB_INFO_TABLE_NAME)
