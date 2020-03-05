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

Status GcsStorageRedisClient::Put(const std::string &table, const std::string &key,
                                  const std::string &value, const SetCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Get(const std::string &table, const std::string &key,
                                  const GetCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::GetByIndex(const std::string &table,
                                         const std::string &index, const std::string &key,
                                         const GetCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::GetAll(const std::string &table,
                                     const GetAllCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Delete(const std::string &table, const std::string &key,
                                     const DeleteCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Delete(const std::string &table,
                                     const std::vector<std::string> &keys,
                                     const DeleteCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::DeleteByIndex(const std::string &table, const int &index,
                                            const std::string &key,
                                            const DeleteCallback &callback) {
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
