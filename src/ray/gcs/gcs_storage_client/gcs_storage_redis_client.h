#ifndef RAY_GCS_GCS_STORAGE_REDIS_CLIENT_H_
#define RAY_GCS_GCS_STORAGE_REDIS_CLIENT_H_

#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {
namespace gcs {

class GcsStorageRedisClient : public GcsStorageClient {
 public:
  GcsStorageRedisClient(const GcsStorageRedisClientConfig &config);

  virtual ~GcsStorageRedisClient();

  Status Connect(boost::asio::io_service &io_service) override;

  void Disconnect() override;

  Status Put(const std::string &table, const std::string &key, const std::string &value,
             const SetCallback &callback) override;

  Status Put(const std::string &table, const std::string &index, const std::string &key,
             const std::string &value, const SetCallback &callback) override;

  Status Get(const std::string &table, const std::string &key,
             const GetCallback &callback) override;

  Status GetByIndex(const std::string &table, const std::string &index,
                    const std::string &key, const GetCallback &callback) override;

  Status GetAll(const std::string &table, const GetAllCallback &callback) override;

  Status Delete(const std::string &table, const std::string &key,
                const DeleteCallback &callback) override;

  Status Delete(const std::string &table, const std::vector<std::string> &keys,
                const DeleteCallback &callback) override;

  Status DeleteByIndex(const std::string &table, const int &index, const std::string &key,
                       const DeleteCallback &callback) override;

 private:
  /// Gcs storage client configuration
  GcsStorageClientConfig config_;
  std::unique_ptr<RedisGcsClient> redis_gcs_client_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_STORAGE_REDIS_CLIENT_H_