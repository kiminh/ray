#pragma once

#include <memory>
#include <unordered_set>
#include "abstract_failover.h"
#include "ray/failure_detector/failure_detector.h"

using FailureDetector = ray::fd::FailureDetector;

namespace ray {
namespace gcs {

class L1Failover : public AbstractFailover {
 public:
  explicit L1Failover(boost::asio::io_context &ioc,
                      const std::shared_ptr<FailureDetector> &fd)
      : ioc_(ioc), fd_(fd) {
    fd_->OnWorkerConnected([this](ray::fd::WorkerContext &&ctx) {
      OnWorkerConnected(ctx.node_id, ctx.endpoint);
    });

    fd_->OnWorkerDisconnected([this](std::vector<ray::fd::WorkerContext> &&ctxs) {
      for (auto &ctx : ctxs) {
        OnWorkerDisconnected(ctx.node_id, ctx.endpoint);
      }
    });

    fd_->OnWorkerRestartedWithinLease([this](ray::fd::WorkerContext &&ctx) {
      OnWorkerRestarted(ctx.node_id, ctx.endpoint);
    });

    fd_->Start(3, 3, 14, 15);
  }

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
  std::shared_ptr<FailureDetector> fd_;
  std::unordered_set<uint64_t> round_failed_nodes_;
  std::unordered_set<std::shared_ptr<boost::asio::deadline_timer>> timers_;
};

}  // namespace gcs
}  // namespace ray
