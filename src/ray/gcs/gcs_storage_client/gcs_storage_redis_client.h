#ifndef RAY_GCS_GCS_STORAGE_REDIS_CLIENT_H_
#define RAY_GCS_GCS_STORAGE_REDIS_CLIENT_H_

#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"

namespace ray {
namespace gcs {

class GcsStorageRedisClient : public GcsStorageClient {
 public:
  GcsStorageRedisClient(const GcsStorageClientConfig &config);

  virtual ~GcsStorageRedisClient();

  Status Connect() override;

  void Disconnect() override;

  Status Get(const std::string &key, const GetCallback &callback) override;

  Status GetAll(const std::string &index, const GetAllCallback &callback) override;

  Status Set(const std::string &key, const std::string &value,
             const SetCallback &callback) override;

  Status Set(const std::string &index, const std::string &key, const std::string &value,
             const SetCallback &callback) override;

  Status Delete(const std::string &key, const DeleteCallback &callback) override;

  Status Delete(const std::vector<std::string> &keys,
                const DeleteCallback &callback) override;

 private:
  /// Gcs storage client configuration
  GcsStorageClientConfig config_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_STORAGE_REDIS_CLIENT_H_
