#include "gcs_storage_redis_client.h"

namespace ray {
namespace gcs {

GcsStorageRedisClient::GcsStorageRedisClient(const GcsStorageRedisClientConfig &config)
    : GcsStorageClient(config) {
  GcsClientOptions options(config.server_ip_, config.server_port_, config.password_,
                           config.is_test_client_);
  redis_gcs_client_ = std::unique_ptr<RedisGcsClient>(new RedisGcsClient(options));
}

Status GcsStorageRedisClient::Connect(boost::asio::io_service &io_service) {
  return redis_gcs_client_->Connect(io_service);
}

void GcsStorageRedisClient::Disconnect() { redis_gcs_client_->Disconnect(); }

Status GcsStorageRedisClient::Get(const int &index, const std::string &key,
                                  const GetCallback &callback) {
  //  auto on_done = [callback](const std::vector<std::string> &data) {
  //    boost::optional<std::string> result;
  //    if (!data.empty()) {
  //      result = data.back();
  //    }
  //    callback(Status::OK(), result);
  //  };

  Status status;
  //  switch (index) {
  //    case TablePrefix::ACTOR:
  //      status = redis_gcs_client_->actor_table().Lookup(key, on_done);
  //      break;
  //    case TablePrefix::ACTOR_CHECKPOINT:
  //      status = redis_gcs_client_->actor_checkpoint_table().Lookup(key, on_done);
  //      break;
  //    case TablePrefix::JOB:
  //      status = redis_gcs_client_->job_table().Lookup(key, on_done);
  //      break;
  //  }
  return status;
}

Status GcsStorageRedisClient::GetAll(const int &index, const GetAllCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Set(const int &index, const std::string &key,
                                  const std::string &value, const SetCallback &callback) {
  //  auto on_done = [callback](const Status &status) {
  //    callback(status);
  //  };

  Status status;
  //  switch (index) {
  //    case TablePrefix::ACTOR:
  //      status = redis_gcs_client_->actor_table().Append(key, value, on_done);
  //      break;
  //    case TablePrefix::ACTOR_CHECKPOINT:
  //      status = redis_gcs_client_->actor_checkpoint_table().Add(key, value, on_done);
  //      break;
  //    case TablePrefix::ACTOR_CHECKPOINT_ID:
  //      status = redis_gcs_client_->actor_checkpoint_id_table().Add(key, value,
  //      on_done); break;
  //    case TablePrefix::JOB:
  //      status = redis_gcs_client_->job_table().Append(key, value, on_done);
  //      break;
  //  }
  return status;
}

Status GcsStorageRedisClient::Delete(const int &index, const std::string &key,
                                     const DeleteCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Delete(const int &index,
                                     const std::vector<std::string> &keys,
                                     const DeleteCallback &callback) {
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
