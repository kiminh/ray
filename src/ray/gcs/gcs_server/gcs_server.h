#pragma once

#include "failover/abstract_failover.h"
#include "ray/common/client_connection.h"
#include "ray/failure_detector/failure_detector_master.h"
#include "ray/gcs/gcs_gc_manager.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/http/http_client.h"
#include "ray/http/http_router.h"
#include "ray/http/http_server.h"
#include "ray/raylet/monitor.h"
#include "ray/rpc/gcs/gcs_server.h"

#include <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;

using FailureDetectMaster = ray::fd::FailureDetectorMaster;

namespace ray {
namespace gcs {

class GcsServer : public rpc::GcsServerHandler {
 public:
  explicit GcsServer(const std::string &name, uint16_t port,
                     boost::asio::io_context &ioc);
  virtual ~GcsServer() = default;

 public:
  /// Make `Start` virtual so that we can mock it when write unit test
  virtual void Start();
  void Stop();

  void HandleRegister(const rpc::RegisterRequest &request, rpc::RegisterReply *reply,
                      rpc::SendReplyCallback send_reply_callback) override;

  uint16_t GetPort() const { return gcs_server_.GetPort(); }

 protected:
  /// Make `StartMonitor` virtual so that we can mock it when write unit test
  virtual void StartMonitor();
  void InitGcsClient();
  void InitGcsGCManager();
  void UpdateGcsServerAddress();
  bool IsGcsServerAddressExist();

  void StopMonitor();
  void StartServer();
  void StartFd();
  void StartHttpServer();

  void ClearnGcs();

 private:
  boost::asio::io_context &ioc_;
  FailureDetectMaster fd_;
  std::unique_ptr<AbstractFailover> failover_;
  std::shared_ptr<ray::HttpServer> http_server_;
  rpc::AsioRpcServer gcs_server_;
  rpc::GcsAsioRpcService gcs_service_;
  std::unique_ptr<ray::raylet::Monitor> monitor_;
  std::shared_ptr<RedisGcsClient> gcs_client_;
  std::shared_ptr<GcsGCManager> gcs_gc_manager_;
};

}  // namespace gcs
}  // namespace ray
