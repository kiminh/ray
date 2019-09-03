#include "ray/gcs/gcs_gc_manager.h"

#include <chrono>
#include "ray/common/ray_config.h"

namespace ray {

namespace gcs {

GcsGCManager::GcsGCManager(RedisGcsClient &gcs_client) : gcs_client_(gcs_client) {}

Status GcsGCManager::CleanForLevelOneFailover() {
  std::vector<std::function<Status()>> funcs;
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllActorCheckpointIdData, this));
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllActorCheckpointData, this));
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllTaskLeaseData, this));
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllTaskReconstructionData, this));
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllObjectData, this));
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllErrorData, this));
  funcs.emplace_back(std::bind(&GcsGCManager::CleanAllProfileData, this));

  std::chrono::milliseconds time_ms(100);
  size_t max_retry_times = 3;
  Status status = Status::OK();
  for (auto &fun : funcs) {
    if (!status.ok()) {
      return status;
    }
    for (size_t i = 0; i < max_retry_times; ++i) {
      // Retry 3 times.
      status = fun();
      if (status.ok()) {
        break;
      }
      std::this_thread::sleep_for(time_ms);
    }
  }

  return Status::OK();
}

Status GcsGCManager::CleanAllJobData() {
  auto &job_table = gcs_client_.job_table();
  return job_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllActorData() {
  auto &actor_table = gcs_client_.actor_table();
  return actor_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllActorCheckpointData() {
  auto &checkpoint_table = gcs_client_.actor_checkpoint_table();
  return checkpoint_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllActorCheckpointIdData() {
  auto &checkpoint_id_table = gcs_client_.actor_checkpoint_id_table();
  return checkpoint_id_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllTaskData() {
  auto &task_table = gcs_client_.raylet_task_table();
  return task_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllTaskLeaseData() {
  auto &task_lease_table = gcs_client_.task_lease_table();
  return task_lease_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllTaskReconstructionData() {
  auto &task_reconstruction_table = gcs_client_.task_reconstruction_log();
  return task_reconstruction_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllObjectData() {
  auto &object_table = gcs_client_.object_table();
  return object_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllErrorData() {
  auto &error_table = gcs_client_.error_table();
  return error_table.SyncDeleteAll();
}

Status GcsGCManager::CleanAllProfileData() {
  auto &profile_table = gcs_client_.profile_table();
  return profile_table.SyncDeleteAll();
}

}  // namespace gcs

}  // namespace ray
