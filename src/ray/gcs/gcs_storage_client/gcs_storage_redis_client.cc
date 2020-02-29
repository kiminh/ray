#include "ray/gcs/gcs_storage_client/gcs_storage_redis_client.h"

namespace ray {
namespace gcs {

GcsStorageRedisClient::GcsStorageRedisClient(const GcsStorageClientConfig &config)
    : GcsStorageClient(config) {}

Status GcsStorageRedisClient::Connect() { return Status::OK(); }

void GcsStorageRedisClient::Disconnect() {}

Status GcsStorageRedisClient::Get(const std::string &key, const GetCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::GetAll(const std::string &index,
                                     const GetAllCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Set(const std::string &key, const std::string &value,
                                  const SetCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Set(const std::string &index, const std::string &key,
                                  const std::string &value, const SetCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Delete(const std::string &key,
                                     const DeleteCallback &callback) {
  return Status::OK();
}

Status GcsStorageRedisClient::Delete(const std::vector<std::string> &keys,
                                     const DeleteCallback &callback) {
  return Status::OK();
}

}  // namespace gcs
}  // namespace ray
