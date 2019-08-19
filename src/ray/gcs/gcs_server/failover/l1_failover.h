#pragma once

#include <memory>
#include <unordered_set>
#include "abstract_failover.h"
#include "ray/failure_detector/failure_detector_master.h"

namespace ray {
namespace gcs {

class L1Failover : public AbstractFailover {
 public:
  explicit L1Failover(boost::asio::io_context &ioc, fd::FailureDetectorMaster &fd);

  bool TryRegister(const rpc::RegisterRequest &req, rpc::RegisterReply *reply) override;
  void OnNodeFailed(const NodeContext &ctx) override;

 private:
  void AcceptNode(const NodeContext &ctx);
  void OnRoundFailedBegin(const NodeContext &ctx);
  void OnRoundFailedEnd(const NodeContext &ctx);

  void ResetNode(const NodeContext &ctx);

 private:
  boost::asio::io_context &ioc_;
  fd::FailureDetectorMaster &fd_;
  std::unordered_set<uint64_t> round_failed_nodes_;
};

}  // namespace gcs
}  // namespace ray
