#pragma once

#include "failover.h"
#include "ray/failure_detector/failure_detector.h"

namespace ray {
namespace raylet {

class L1Failover : public Failover {
 public:
  explicit L1Failover(const std::shared_ptr<fd::FailureDetector> &fd);
  void OnWorkerExit(Worker *worker) override;
  ray::Status OnResetState(const ray::rpc::ResetStateRequest &request,
                           ray::rpc::ResetStateReply *reply) override;

 protected:
  void OnMasterDisconnected(const ip::detail::endpoint &endpoint);

 private:
  void DoExit(int code);
};

}  // namespace raylet
}  // namespace ray