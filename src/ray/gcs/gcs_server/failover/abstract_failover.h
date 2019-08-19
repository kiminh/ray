#pragma once

#include <memory>
#include <unordered_map>
#include "failover.h"

#include <ray/common/status.h>
#include <ray/protobuf/gcsx.pb.h>

namespace ray {
namespace gcs {

class AbstractFailover : public Failover {
 public:
  struct NodeContext {
    uint64_t secret = 0;
    uint64_t node_id;
    net::ip::detail::endpoint ep;
  };

 public:
  virtual bool TryRegister(const rpc::RegisterRequest &req,
                           rpc::RegisterReply *reply) = 0;

  void OnWorkerDisconnected(const uint64_t &node_id,
                            const net::ip::detail::endpoint &ep) override;

  void OnWorkerConnected(const uint64_t &node_id,
                         const net::ip::detail::endpoint &ep) override;

  void OnWorkerRestarted(const uint64_t &node_id,
                         const net::ip::detail::endpoint &ep) override;

  void OnRoundFailedBegin(std::function<void()> &&fn) {
    on_round_failed_begin_ = std::move(fn);
  }

  void OnRoundFailedEnd(std::function<void()> &&fn) {
    on_round_failed_end_ = std::move(fn);
  }

 protected:
  virtual void OnNodeFailed(const NodeContext &ctx) = 0;

 protected:
  std::function<void()> on_round_failed_begin_;
  std::function<void()> on_round_failed_end_;
  std::unordered_map<uint64_t, NodeContext> registered_nodes_;
};

}  // namespace gcs
}  // namespace ray
