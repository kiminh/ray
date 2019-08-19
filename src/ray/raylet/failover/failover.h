#pragma once

#include "ray/failure_detector/failure_detector.h"
#include "ray/protobuf/failover.pb.h"
#include "ray/rpc/failover/failover_server.h"
#include "src/ray/raylet/worker.h"

namespace ray {
namespace raylet {

class Failover : public rpc::FailoverHandler {
 public:
  explicit Failover(const std::shared_ptr<fd::FailureDetector> &fd) {
    fd->OnMasterDisconnected([this](const ip::detail::endpoint &endpoint) {
      this->OnMasterDisconnected(endpoint);
    });
    failover_service_ = std::unique_ptr<rpc::FailoverAsioRpcService>(
        new rpc::FailoverAsioRpcService(*this));
  }
  virtual ~Failover() = default;

  virtual void OnWorkerExit(Worker *worker) = 0;
  virtual void OnMasterDisconnected(const ip::detail::endpoint &endpoint) = 0;

  void RegisterServiceTo(ray::rpc::AsioRpcServer &server) {
    server.RegisterService(*failover_service_);
  }

  uint64_t GetSecret() const { return secret_; }

 protected:
  /// Secret
  uint64_t secret_ = current_sys_time_ns();
  /// The node manager RPC service.
  std::unique_ptr<rpc::FailoverAsioRpcService> failover_service_;
};

}  // namespace raylet
}  // namespace ray
