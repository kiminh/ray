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

    for (size_t i = 0; i < total_count; ++i) {
      std::shared_ptr<ObjectTableData> object = std::make_shared<ObjectTableData>();
      object->set_object_size(i);
      object->set_manager("10.10.10.10");
      ObjectID object_id = ObjectID::FromRandom();
      object_id_to_data_[object_id] = object;
    }
  }

  std::unique_ptr<GcsGCManager> gc_manager_;
  std::unordered_map<ObjectID, std::shared_ptr<ObjectTableData>> object_id_to_data_;
};

TEST_F(GcsGCManagerTest, CleanAllActorDataTest) {
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
  RAY_CHECK_OK(gc_manager_->CleanAllActorData());

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

TEST_F(GcsGCManagerTest, CleanForLevelOneFailover) {
  auto &object_table = gcs_client_->object_table();
  // add
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    RAY_CHECK_OK(
        object_table.Add(JobID::Nil(), elem.first, elem.second,
                         [this](RedisGcsClient *client, const ObjectID &id,
                                const ObjectTableData &data) { --pending_count_; }));
  }
  WaitPendingDone(wait_pending_timeout_);

  // clean
  RAY_CHECK_OK(gc_manager_->CleanForLevelOneFailover());

  // get
  for (const auto &elem : object_id_to_data_) {
    ++pending_count_;
    RAY_CHECK_OK(object_table.Lookup(JobID::Nil(), elem.first,
                                     [this](RedisGcsClient *client, const ObjectID &id,
                                            const std::vector<ObjectTableData> &data) {
                                       RAY_CHECK(data.empty()) << data.size();
                                       --pending_count_;
                                     }));
  }
  WaitPendingDone(wait_pending_timeout_);
}

}  // namespace gcs

}  // namespace ray
