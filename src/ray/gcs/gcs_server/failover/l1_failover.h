#pragma once

#include "abstract_failover.h"
#include "ray/failure_detector/failure_detector.h"
#include <unordered_set>
#include <memory>

using FailureDetector = ray::failure_detector::FailureDetector;

namespace ray {

namespace gcs {

class L1Failover : public AbstractFailover {
 public:
  explicit L1Failover(const std::shared_ptr<FailureDetector>& fd)
    : fd_(fd) {
  }

  void TryRegister(const rpc::RegisterRequest &req,
                   rpc::RegisterReply &reply) override;
  void OnNodeFailed(const NodeContext& ctx) override;

 private:
  void AcceptNode(const NodeContext &ctx);
  void OnRoundFailedBegin(const NodeContext& ctx);
  void OnRoundFailedEnd(const NodeContext& ctx);

  void ResetNode(const NodeContext& ctx);

 private:
  std::shared_ptr<FailureDetector> fd_;
  std::unordered_set<std::string> round_failed_nodes_;
};

}

}
