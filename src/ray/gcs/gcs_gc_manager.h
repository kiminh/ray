#ifndef RAY_GCS_GCS_GC_MANAGER_H
#define RAY_GCS_GCS_GC_MANAGER_H

#include "ray/gcs/redis_gcs_client.h"

namespace ray {

namespace gcs {

class GcsGCManager {
 public:
  explicit GcsGCManager(RedisGcsClient &gcs_client);

  ~GcsGCManager() {}

  /// Clean all jobs from GCS synchronously. Those data includes job informations,
  /// task informations, actor informations, object informations and so on.
  ///
  /// \return Status
  // TODO(micafan) Confirm what kind of data needed to delete in level One FO.
  Status CleanAllJobs();

 private:
  /// Clean all data from GCS JobTable.
  ///
  /// \return Status
  Status CleanAllJobData();

  /// Clean all data from GCS ActorTable.
  ///
  /// \return Status
  Status CleanAllActorData();

  /// Clean all data from GCS ActorCheckpointTable.
  ///
  /// \return Status
  Status CleanAllActorCheckpointData();

  /// Clean all data from GCS ActorCheckpointIdTable.
  ///
  /// \return Status
  Status CleanAllActorCheckpointIdData();

  /// Clean all data from GCS TaskTable.
  ///
  /// \return Status
  Status CleanAllTaskData();

  /// Clean all data from GCS TaskLeaseTable.
  ///
  /// \return Status
  Status CleanAllTaskLeaseData();

  /// Clean all data from GCS TaskReconstructionTable.
  ///
  /// \return Status
  Status CleanAllTaskReconstructionData();

  /// Clean all data from GCS ObjectTable.
  ///
  /// \return Status
  Status CleanAllObjectData();

  /// Clean all data from GCS ErrorTable.
  ///
  /// \return Status
  Status CleanAllErrorData();

  /// Clean all data from GCS ProfileTable.
  ///
  /// \return Status
  Status CleanAllProfileData();

  RedisGcsClient &gcs_client_;
};

}  // namespace gcs

}  // namespace ray

#endif  // RAY_GCS_GCS_GC_MANAGER_H
