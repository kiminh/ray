#include "gtest/gtest.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/test_util.h"

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "ray/protobuf/gcs.pb.h"

#include "ray/gcs/gcs_client/gcs_table_sub.h"

namespace ray {

class GcsTableSubTest : public RedisServiceManagerForTest {
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

TEST_F(GcsTableSubTest, TestApi) {
  RAY_LOG(INFO) << "GcsTableSubTest......";
  gcs::GcsJobTableSub table_sub(client_);
  JobID job_id;
  ClientID client_id;
  auto subscribe = [](const JobID &id, const std::vector<rpc::JobTableData> &data) {
  };
  auto done = [](Status status) {
  };
  RAY_CHECK_OK(table_sub.Subscribe(job_id, client_id, subscribe, done));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}
