#include <ray/protobuf/gcs.pb.h>
#include "gtest/gtest.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/test_util.h"

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "ray/protobuf/gcs.pb.h"

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

    gcs::RedisClientOptions redis_client_options("127.0.0.1", REDIS_SERVER_PORT, "", true);
    client_ = std::make_shared<gcs::RedisClient>(redis_client_options);
    Status status = client_->Connect(io_service_);
    RAY_CHECK_OK(status);
  }

  virtual void TearDown() override {
    io_service_.stop();
    thread_io_service_->join();
  }

 protected:
  std::shared_ptr<gcs::RedisClient> client_;

  boost::asio::io_service io_service_;
  std::unique_ptr<std::thread> thread_io_service_;
};

TEST_F(GcsTablePubSubTest, TestApi) {
  RAY_LOG(INFO) << "GcsTablePubSubTest......";
  gcs::GcsJobTablePubSub table_sub(client_);
  JobID job_id = JobID::FromInt(1);
  ClientID client_id;

  std::promise<bool> promise;
  auto subscribe = [&promise](const JobID &id, const std::vector<rpc::JobTableData> &data) {
    RAY_LOG(INFO) << "subscribe message......, data size = " << data.size();
    auto job_id = JobID::FromBinary(data[0].job_id());
    RAY_LOG(INFO) << "subscribe message, job id = " << job_id;
//    ASSERT_EQ(data.size(), 1);
    promise.set_value(true);
  };
  auto done = [](Status status) {
  };
  RAY_CHECK_OK(table_sub.Subscribe(job_id, client_id, subscribe, done));

  int job_pubsub = rpc::TablePubsub::JOB_PUBSUB;
  std::vector<std::string> args;
  args.push_back("PUBLISH");
  args.push_back(std::to_string(job_pubsub));
  auto context = client_->GetPrimaryContext();

  rpc::JobTableData job_table_data;
  job_table_data.set_job_id(job_id.Binary());
  std::string job_table_data_str;
  job_table_data.SerializeToString(&job_table_data_str);

  rpc::GcsEntry gcs_entry;
  gcs_entry.set_id(job_id.Binary());
  gcs_entry.set_change_mode(rpc::GcsChangeMode::APPEND_OR_ADD);
  gcs_entry.add_entries(job_table_data_str);
  std::string gcs_entry_str = gcs_entry.SerializeAsString();
  args.push_back(gcs_entry_str);
  RAY_CHECK_OK(context->RunArgvAsync(args, nullptr));
  promise.get_future().get();
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
