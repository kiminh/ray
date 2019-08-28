#pragma once

#include <memory>
#include <unordered_set>
#include "abstract_failover.h"
#include "ray/failure_detector/failure_detector.h"
#include "ray/raylet/monitor.h"

using FailureDetector = ray::fd::FailureDetector;

namespace ray {
namespace gcs {

class L1Failover : public AbstractFailover {
 public:
  explicit L1Failover(boost::asio::io_context &ioc, FailureDetector &fd);

  ray::Status TryRegister(const rpc::RegisterRequest &req,
                          rpc::RegisterReply *reply) override;
  void OnNodeFailed(const NodeContext &ctx) override;

 private:
  void AcceptNode(const NodeContext &ctx);
  void OnRoundFailedBegin(const NodeContext &ctx);
  void OnRoundFailedEnd(const NodeContext &ctx);

  void ResetNode(const NodeContext &ctx);

 private:
  boost::asio::io_context &ioc_;
  FailureDetector &fd_;
  std::unordered_set<uint64_t> round_failed_nodes_;
  std::unordered_set<std::shared_ptr<boost::asio::deadline_timer>> timers_;
  std::unique_ptr<ray::raylet::Monitor> monitor_;
};

}  // namespace gcs
}  // namespace ray
