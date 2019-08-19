#include "abstract_failover.h"
#include "ray/util/logging.h"

namespace ray {

namespace gcs {

void AbstractFailover::OnWorkerDisconnected(const std::string &node_id,
                                          const net::ip::detail::endpoint &ep) {
  RAY_LOG(WARNING) << "node_id: " << node_id << ", address: " << ep.to_string();
  auto iter = registered_nodes_.find(node_id);
  if (iter == registered_nodes_.end()) {
    RAY_LOG(WARNING) << "node_id: " << node_id << ", address: " << ep.to_string()
                     << ", just ignore this disconnection as this node"
                     << " has not yet registered!";
    return;
  }

  OnNodeFailed(iter->second);
}

void AbstractFailover::OnWorkerConnected(const std::string &node_id,
                                       const net::ip::detail::endpoint &ep) {
  RAY_LOG(WARNING) << "node_id: " << node_id << ", address: " << ep.to_string();
  auto iter = registered_nodes_.find(node_id);
  if (iter != registered_nodes_.end()) {
    const auto& old = iter->second;
    RAY_LOG(WARNING) << "node_id: " << node_id << " address is changed,"
                     << " old: " << old.ep.to_string() << ", new: " << ep.to_string();
    OnNodeFailed(iter->second);
  }
}

void AbstractFailover::OnWorkerRestarted(const std::string &node_id,
                                       const net::ip::detail::endpoint &ep) {
  RAY_LOG(WARNING) << "node_id: " << node_id << ", address: " << ep.to_string();
  auto iter = registered_nodes_.find(node_id);
  if (iter == registered_nodes_.end()) {
    RAY_LOG(WARNING) << " node_id: " << node_id << ", address " << ep.to_string()
                     << ", just ignore this disconnection as this node"
                     << " has not yet acquired token";
    return;
  }

  OnNodeFailed(iter->second);
}

}

}
