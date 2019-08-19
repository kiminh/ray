#pragma once

#include "failover.h"
#include <unordered_map>
#include <memory>
#include <ray/protobuf/gcs_server.pb.h>

namespace ray {

namespace gcs {

class AbstractFailover : public Failover {
 public:
  struct NodeContext {
    uint64_t secret = 0;
    std::string node_id;
    net::ip::detail::endpoint ep;
  };

 public:
  virtual void TryRegister(const rpc::RegisterRequest &req,
                           rpc::RegisterReply &reply) = 0;

  void OnWorkerDisconnected(
      const std::string &node_id,
      const net::ip::detail::endpoint &ep) override;

  void OnWorkerConnected(
      const std::string &node_id,
      const net::ip::detail::endpoint &ep) override;

  void OnWorkerRestarted(
      const std::string &node_id,
      const net::ip::detail::endpoint &ep) override;

 protected:
  virtual void OnNodeFailed(const NodeContext& ctx) = 0;

 protected:
  std::unordered_map<std::string, NodeContext> registered_nodes_;
};

}

}