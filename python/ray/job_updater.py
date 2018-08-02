from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from enum import Enum

import ray
import ray.services as services
import redis
import time

from ray import local_scheduler
from ray import utils


class JobUpdater(object):
    """A helper class used to add or update job information.

    Attributes:
        worker: the worker object.
    """

    def __init__(self, worker):
        self.worker = worker


    def set_job_state(self, state):
        if self._prechecks() is False:
            return

         # Get job.
        job_id = ray.ObjectID(self.worker.worker_id)
        existing_job = self._get_job_from_GCS(job_id)

        if existing_job is None:
            # Job doesn't exist
            if state is ray.gcs_utils.JobState.Started:
                # Probably we are launching this driver inside Ray cluster,
                # let's create a new job.
                time_now = time.time()
                job = ray.local_scheduler.Job(
                    job_id, "None", "None", services.get_node_ip_address(),
                    ray.ObjectID(ray.NIL_ID), ray.gcs_utils.JobState.Started,
                    time_now, time_now, 0)
            else:
                raise Exception("Could not find Job '{}' in GCS.".format(
                    self.worker.worker_id))
        else:
            if state is ray.gcs_utils.JobState.Started:
                job = ray.local_scheduler.Job(
                    ray.ObjectID(existing_job.Id()), existing_job.Owner(),
                    existing_job.Name(), existing_job.HostServer(), 
                    ray.ObjectID(existing_job.ExecutableId()),
                    ray.gcs_utils.JobState.Started, existing_job.CreateTime(),
                    time.time(), existing_job.end_time())
            elif (state is ray.gcs_utils.JobState.Completed
                or state is ray.gcs_utils.JobState.Timeout
                or state is ray.gcs_utils.JobState.Failed):
                job = ray.local_scheduler.Job(
                    ray.ObjectID(existing_job.Id()), existing_job.Owner(),
                    existing_job.Name(), existing_job.HostServer(),
                    ray.ObjectID(existing_job.ExecutableId()),
                    state, existing_job.CreateTime(),
                    existing_job.EndTime(), time.time())
            else:
                raise Exception("This should never happen.")

        # Set job into GCS, this will update the record if it already exists.
        ray.global_state._execute_command(
            job.id(), "RAY.TABLE_ADD",
            ray.gcs_utils.TablePrefix.JOB,
            ray.gcs_utils.TablePubsub.JOB,
            job.id().id(),
            job.to_serialized_flatbuf())


    def _prechecks(self):
        # Only update job info when it is in driver mode
        # and worker_id is specified.
        if (self.worker.mode not in [ray.SCRIPT_MODE, ray.SILENT_MODE]
            or self.worker.worker_id is None):
            return False
        else:
            return True


    def _get_job_from_GCS(self, job_id):
        data = ray.global_state._execute_command(job_id, "RAY.TABLE_LOOKUP",
            ray.gcs_utils.TablePrefix.JOB, "", job_id.id())

        if data is None:
            return None

        gcs_entries = ray.gcs_utils.GcsTableEntry.GetRootAsGcsTableEntry(
            data, 0)
        return ray.gcs_utils.Job.GetRootAsJob(gcs_entries.Entries(0), 0)
