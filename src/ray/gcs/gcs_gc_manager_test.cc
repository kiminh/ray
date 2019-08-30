#include "ray/gcs/gcs_gc_manager.h"
#include "gtest/gtest.h"
#include "ray/common/ray_config.h"
#include "ray/gcs/accessor_test_base.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/util/test_util.h"

namespace ray {

namespace gcs {

class GcsGCManagerTest : public AccessorTestBase<ActorID, ActorTableData> {
 public:
  void SetUp() {
    AccessorTestBase::SetUp();
    gc_manager_.reset(new GcsGCManager(*gcs_client_));
  }

 protected:
  virtual void GenTestData() {
    size_t total_count = RayConfig::instance().maximum_gcs_deletion_batch_size() * 2 + 1;
    for (size_t i = 0; i < total_count; ++i) {
      std::shared_ptr<ActorTableData> actor = std::make_shared<ActorTableData>();
      actor->set_max_reconstructions(1);
      actor->set_remaining_reconstructions(1);
      JobID job_id = JobID::FromInt(i);
      actor->set_job_id(job_id.Binary());
      actor->set_state(ActorTableData::ALIVE);
      ActorID actor_id = ActorID::Of(job_id, RandomTaskId(), /*parent_task_counter=*/i);
      actor->set_actor_id(actor_id.Binary());
      id_to_data_[actor_id] = actor;
    }
  }

  std::unique_ptr<GcsGCManager> gc_manager_;
};

TEST_F(GcsGCManagerTest, CleanAllJobsTest) {
  ActorStateAccessor &actor_accessor = gcs_client_->Actors();
  // register
  for (const auto &elem : id_to_data_) {
    const auto &actor = elem.second;
    ++pending_count_;
    RAY_CHECK_OK(actor_accessor.AsyncRegister(actor, [this](Status status) {
      RAY_CHECK_OK(status);
      --pending_count_;
    }));
  }

  WaitPendingDone(wait_pending_timeout_);

  // clean
  RAY_CHECK_OK(gc_manager_->CleanAllJobs());

  // get
  for (const auto &elem : id_to_data_) {
    ++pending_count_;
    RAY_CHECK_OK(actor_accessor.AsyncGet(
        elem.first, [this](Status status, const boost::optional<ActorTableData> &data) {
          ASSERT_TRUE(!data);
          --pending_count_;
        }));
  }

  WaitPendingDone(wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray
