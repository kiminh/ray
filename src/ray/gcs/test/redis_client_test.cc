#include "gtest/gtest.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/redis_client.h"
#include "ray/gcs/redis_context.h"
#include "ray/util/test_util.h"

#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/util/logging.h"
#include "ray/protobuf/gcs.pb.h"

namespace ray {
namespace gcs {

class RedisClientTest : public RedisServiceManagerForTest {
 public:
  RedisClientTest() {}

  virtual void SetUp() override {
    thread_io_service_.reset(new std::thread([this] {
      std::unique_ptr<boost::asio::io_service::work> work(
          new boost::asio::io_service::work(io_service_));
      io_service_.run();
    }));

    RedisClientOptions redis_client_options("127.0.0.1", REDIS_SERVER_PORT, "", true);
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

TEST_F(RedisClientTest, ApiTest) {
  RAY_LOG(INFO) << "RedisClientTest....";
  auto context = client_->GetPrimaryContext();
  std::vector<std::string> args;
  args.push_back("SUBSCRIBE");
  args.push_back("CHANNEL1");

  std::promise<bool> promise;
  RedisCallback redis_callback = [&promise](std::shared_ptr<CallbackReply> reply) {
    RAY_LOG(INFO) << "hello world................";
    if (!reply->IsNil()) {
      RAY_LOG(INFO) << "!reply->IsNil()................";
      const auto data = reply->ReadAsPubsubData();

      if (data.empty()) {
        RAY_LOG(INFO) << "data is empty.........";
      } else {
        RAY_LOG(INFO) << "data is = " << data;
        promise.set_value(true);
//        // Parse the notification.
//        rpc::GcsEntry gcs_entry;
//        gcs_entry.ParseFromString(data);
//        ID id = ID::FromBinary(gcs_entry.id());
//        std::vector<Data> results;
//        for (int64_t i = 0; i < gcs_entry.entries_size(); i++) {
//          Data result;
//          result.ParseFromString(gcs_entry.entries(i));
//          results.emplace_back(std::move(result));
//        }
//        RAY_LOG(INFO) << "result is = " << result;
      }

    }

  };
  ClientID client_id = ClientID::Nil();
  int64_t index;
  RAY_CHECK_OK(context->SubscribeAsync(client_id, TablePubsub::JOB_PUBSUB, redis_callback, &index));

  args.clear();
  int job_pubsub = TablePubsub::JOB_PUBSUB;
  args.push_back("PUBLISH");
  args.push_back(std::to_string(job_pubsub));
  args.push_back("hello world");
  RAY_CHECK_OK(context->RunArgvAsync(args, nullptr));
  promise.get_future().get();
}

}  // namespace gcs
}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 3);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  return RUN_ALL_TESTS();
}