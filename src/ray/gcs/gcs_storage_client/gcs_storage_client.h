#ifndef RAY_GCS_GCS_STORAGE_CLIENT_H_
#define RAY_GCS_GCS_STORAGE_CLIENT_H_

#include <boost/optional/optional.hpp>
#include "ray/common/status.h"

namespace ray {
namespace gcs {

struct GcsStorageClientConfig {
  bool is_test = false;
};

using GetCallback =
    std::function<void(const Status &status, const boost::optional<std::string> &data)>;
using GetAllCallback =
    std::function<void(const Status &status, const std::vector<std::string> &data)>;
using SetCallback = std::function<void(const Status &status)>;
using DeleteCallback = std::function<void(const Status &status)>;

class GcsStorageClient {
 public:
  GcsStorageClient(const GcsStorageClientConfig &config) : config_(config) {}

  virtual ~GcsStorageClient();

  virtual Status Connect();

  virtual void Disconnect();

  virtual Status Get(const std::string &key, const GetCallback &callback);

  virtual Status GetAll(const std::string &index, const GetAllCallback &callback);

  virtual Status Set(const std::string &key, const std::string &value,
                     const SetCallback &callback);

  virtual Status Set(const std::string &index, const std::string &key,
                     const std::string &value, const SetCallback &callback);

  virtual Status Delete(const std::string &key, const DeleteCallback &callback);

  virtual Status Delete(const std::vector<std::string> &keys,
                        const DeleteCallback &callback);

 private:
  /// Gcs storage client configuration
  GcsStorageClientConfig config_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_STORAGE_CLIENT_H_
