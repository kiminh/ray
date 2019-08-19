#pragma once

#include "failover.h"
#include "ray/failure_detector/failure_detector.h"

namespace ray {
namespace raylet {

class L1Failover : public Failover {
 public:
  explicit L1Failover(const std::shared_ptr<fd::FailureDetector> &fd) : Failover(fd) {}

  void OnWorkerExit(Worker *worker) override;

  void HandleResetState(const ray::rpc::ResetStateRequest &request,
                        ray::rpc::ResetStateReply *reply,
                        ray::rpc::SendReplyCallback send_reply_callback) override;

 protected:
  void OnMasterDisconnected(const ip::detail::endpoint &endpoint) override;

 protected:
  virtual void DoExit(int code);
};

}  // namespace raylet
}  // namespace ray