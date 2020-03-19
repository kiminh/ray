#include "gtest/gtest.h"
#include "ray/gcs/gcs_server/gcs_table_storage.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/util/test_util.h"

namespace ray {

class GcsTableStorageTest : public RedisServiceManagerForTest {
 public:
  void SetUp() override {
  }

  void TearDown() override {
  }

 protected:

  std::shared_ptr<rpc::JobTableData> GenJobTableData(JobID job_id) {
    auto job_table_data = std::make_shared<rpc::JobTableData>();
    job_table_data->set_job_id(job_id.Binary());
    job_table_data->set_is_dead(false);
    job_table_data->set_timestamp(std::time(nullptr));
    job_table_data->set_node_manager_address("127.0.0.1");
    job_table_data->set_driver_pid(5667L);
    return job_table_data;
  }
};

TEST_F(GcsTableStorageTest, TestApi) {
  RAY_LOG(INFO) << "GcsTableStorageTest........................";
  gcs::StoreClientOptions config("127.0.0.1", REDIS_SERVER_PORT, "", true);
  auto store_client = std::make_shared<gcs::RedisStoreClient>(config);
  gcs::GcsJobTable job_table(*store_client);
  JobID job_id = JobID::FromInt(1);

  auto on_done = [](Status status) {};
  RAY_CHECK_OK(job_table.Put(job_id, job_id, GenJobTableData(job_id), on_done));
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  RAY_CHECK(argc == 4);
  ray::REDIS_SERVER_EXEC_PATH = argv[1];
  ray::REDIS_CLIENT_EXEC_PATH = argv[2];
  ray::REDIS_MODULE_LIBRARY_PATH = argv[3];
  return RUN_ALL_TESTS();
}
