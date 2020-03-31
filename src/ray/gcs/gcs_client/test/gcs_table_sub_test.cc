#include <ray/protobuf/gcs.pb.h>
#include "gtest/gtest.h"

#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/test_util.h"

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"

#include "ray/gcs/gcs_client/gcs_table_pubsub.h"

namespace ray {

class GcsTablePubSubTest : public RedisServiceManagerForTest {
 public:
  virtual void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    gcs::RedisClientOptions redis_client_options("127.0.0.1", REDIS_SERVER_PORT, "",
                                                 true);
    client_ = std::make_shared<gcs::RedisClient>(redis_client_options);
    Status status = client_->Connect(io_service_);
    RAY_CHECK_OK(status);
  }

  virtual void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

  template <typename TABLE_PUB_SUB, typename ID, typename Data>
  void PubSub(TABLE_PUB_SUB &table_pub_sub, const JobID &job_id, const ClientID client_id,
              const ID &id, const Data &data) {
    std::promise<bool> promise;
    auto subscribe = [&promise](const ID &id, const std::vector<Data> &data) {
      ASSERT_EQ(data.size(), 1);
      promise.set_value(true);
    };
    RAY_CHECK_OK(table_pub_sub.Subscribe(job_id, client_id, id, subscribe, nullptr));
    RAY_CHECK_OK(table_pub_sub.Publish(job_id, client_id, id, data,
                                       rpc::GcsChangeMode::APPEND_OR_ADD, nullptr));
    promise.get_future().get();
  }

 protected:
  std::shared_ptr<gcs::RedisClient> client_;
  JobID job_id_ = JobID::FromInt(1);
  ActorID actor_id_ = ActorID::Of(job_id_, RandomTaskId(), 0);
  TaskID task_id_ = TaskID::ForDriverTask(job_id_);
  ClientID node_id_ = ClientID::FromRandom();

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

TEST_F(GcsTablePubSubTest, TestJobTablePubSubApi) {
  gcs::GcsJobTablePubSub table_pub_sub(client_);
  rpc::JobTableData job_table_data;
  job_table_data.set_job_id(job_id_.Binary());

//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), job_id_, job_table_data);


  int count = 0;
  auto subscribe1 = [&count](const JobID &id, const std::vector<rpc::JobTableData> &data) {
    ++count;
//    ASSERT_EQ(data.size(), 1);
    RAY_LOG(INFO) << "MMMMMMMMMMMMMMM count = " << count << ", data size = " << data.size();
//    promise.set_value(true);
  };
  RAY_CHECK_OK(table_pub_sub.Subscribe(job_id_, ClientID::Nil(), job_id_, subscribe1, nullptr));
  sleep(3);
  RAY_CHECK_OK(table_pub_sub.Unsubscribe(job_id_, ClientID::Nil(), job_id_, nullptr));
//  sleep(3);
////  RAY_CHECK_OK(table_pub_sub.Unsubscribe(job_id_, ClientID::Nil(), job_id_));
//  RAY_CHECK_OK(table_pub_sub.Publish(job_id_, ClientID::Nil(), job_id_, job_table_data,
//                                     rpc::GcsChangeMode::APPEND_OR_ADD, nullptr));

  sleep(3);
  RAY_CHECK_OK(table_pub_sub.Publish(job_id_, ClientID::Nil(), job_id_, job_table_data,
                                     rpc::GcsChangeMode::APPEND_OR_ADD, nullptr));

  sleep(60);
//  promise.get_future().get();
}

//TEST_F(GcsTablePubSubTest, TestActorTablePubSubApi) {
//  gcs::GcsActorTablePubSub table_pub_sub(client_);
//  rpc::ActorTableData actor_table_data;
//  actor_table_data.set_job_id(job_id_.Binary());
//  actor_table_data.set_actor_id(actor_id_.Binary());
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), actor_id_, actor_table_data);
//}
//
//TEST_F(GcsTablePubSubTest, TestTaskTablePubSubApi) {
//  gcs::GcsTaskTablePubSub table_pub_sub(client_);
//  rpc::TaskTableData task_table_data;
//  rpc::Task task;
//  rpc::TaskSpec task_spec;
//  task_spec.set_job_id(job_id_.Binary());
//  task_spec.set_task_id(task_id_.Binary());
//  task.mutable_task_spec()->CopyFrom(task_spec);
//  task_table_data.mutable_task()->CopyFrom(task);
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), task_id_, task_table_data);
//}
//
//TEST_F(GcsTablePubSubTest, TestTaskLeaseTablePubSubApi) {
//  gcs::GcsTaskLeaseTablePubSub table_pub_sub(client_);
//  rpc::TaskLeaseData task_lease_data;
//  task_lease_data.set_task_id(task_id_.Binary());
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), task_id_, task_lease_data);
//}
//
//TEST_F(GcsTablePubSubTest, TestObjectTablePubSubApi) {
//  gcs::GcsObjectTablePubSub table_pub_sub(client_);
//  ObjectID object_id = ObjectID::FromRandom();
//  rpc::ObjectTableData object_table_data;
//  object_table_data.set_manager(node_id_.Binary());
//  object_table_data.set_object_size(1);
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), object_id, object_table_data);
//}
//
//TEST_F(GcsTablePubSubTest, TestNodeTablePubSubApi) {
//  gcs::GcsNodeTablePubSub table_pub_sub(client_);
//  rpc::GcsNodeInfo gcs_node_info;
//  gcs_node_info.set_node_id(node_id_.Binary());
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), node_id_, gcs_node_info);
//}
//
//TEST_F(GcsTablePubSubTest, TestNodeResourceTablePubSubApi) {
//  gcs::GcsNodeResourceTablePubSub table_pub_sub(client_);
//  rpc::ResourceTableData resource_table_data;
//  resource_table_data.set_resource_capacity(1.0);
//  rpc::ResourceMap resource_map;
//  (*resource_map.mutable_items())["node1"] = resource_table_data;
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), node_id_, resource_map);
//}
//
//TEST_F(GcsTablePubSubTest, TestHeartbeatTablePubSubApi) {
//  gcs::GcsHeartbeatTablePubSub table_pub_sub(client_);
//  rpc::HeartbeatTableData heartbeat_table_data;
//  heartbeat_table_data.set_client_id(node_id_.Binary());
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), node_id_, heartbeat_table_data);
//}
//
//TEST_F(GcsTablePubSubTest, TestHeartbeatBatchTablePubSubApi) {
//  gcs::GcsHeartbeatBatchTablePubSub table_pub_sub(client_);
//  rpc::HeartbeatBatchTableData heartbeat_batch_table_data;
//  heartbeat_batch_table_data.add_batch()->set_client_id(node_id_.Binary());
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), node_id_, heartbeat_batch_table_data);
//}
//
//TEST_F(GcsTablePubSubTest, TestWorkerFailureTablePubSubApi) {
//  gcs::GcsWorkerFailureTablePubSub table_pub_sub(client_);
//  WorkerID worker_id = WorkerID::FromRandom();
//  rpc::WorkerFailureData worker_failure_data;
//  worker_failure_data.set_timestamp(std::time(nullptr));
//
//  PubSub(table_pub_sub, job_id_, ClientID::Nil(), worker_id, worker_failure_data);
//}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
