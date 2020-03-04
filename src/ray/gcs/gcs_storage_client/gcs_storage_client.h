#ifndef RAY_GCS_GCS_STORAGE_CLIENT_H_
#define RAY_GCS_GCS_STORAGE_CLIENT_H_

#include <boost/asio.hpp>
#include <boost/optional/optional.hpp>
#include "ray/common/status.h"

namespace ray {
namespace gcs {

struct GcsStorageClientConfig {
  GcsStorageClientConfig(bool is_test_client = false) : is_test_client_(is_test_client) {}

  // Whether this client is used for tests.
  bool is_test_client_ = false;
};

struct GcsStorageRedisClientConfig : GcsStorageClientConfig {
  GcsStorageRedisClientConfig(const std::string &ip, int port,
                              const std::string &password, bool is_test_client = false)
      : server_ip_(ip),
        server_port_(port),
        password_(password),
        GcsStorageClientConfig(is_test_client) {}

  // Redis address.
  std::string server_ip_;
  int server_port_;

  // Password of redis.
  std::string password_;
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

  virtual Status Connect(boost::asio::io_service &io_service);

  virtual void Disconnect();

  virtual Status Put(const std::string &table, const std::string &key, const std::string &value,
                     const SetCallback &callback);

  virtual Status Get(const std::string &table, const std::string &key,
                     const GetCallback &callback);

  virtual Status GetAll(const std::string &table, const GetAllCallback &callback);

  virtual Status Delete(const std::string &table, const std::string &key,
                        const DeleteCallback &callback);

 private:
  /// Gcs storage client configuration
  GcsStorageClientConfig config_;
};

class GcsStorageHelperIndexClient : public GcsStorageClient {
 public:
  GcsStorageHelperIndexClient(const GcsStorageClientConfig &config) : GcsStorageClient(config) {}

  virtual Status Set(const std::string &table, const int &index, const std::string &key, const std::string &value,
                     const SetCallback &callback);

  virtual ~GcsStorageHelperIndexClient();

  virtual Status GetByIndex(const std::string &table, const std::string &index, const std::string &key,
                     const GetCallback &callback);

  virtual Status DeleteByIndex(const std::string &table, const int &index, const std::string &key,
                               const DeleteCallback &callback);
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_GCS_STORAGE_CLIENT_H_
