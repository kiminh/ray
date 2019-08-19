#include "l1_failover.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"


namespace ray {

namespace gcs {

void L1Failover::TryRegister(const ray::rpc::RegisterRequest &req,
                             ray::rpc::RegisterReply &reply) {
  auto ep = endpoint_from_uint64(req.address());
  bool success = false;
  do {
    // Do not issue new token if fo in progress
    if (!round_failed_nodes_.empty()) {
      break;
    }

    // Issue a new token for the node if the node has no token
    auto iter = registered_nodes_.find(req.node_id());
    if (iter == registered_nodes_.end()) {
      NodeContext ctx;
      ctx.node_id = req.node_id();
      ctx.ep = ep;
      ctx.secret = req.secret();
      AcceptNode(ctx);
      success = true;
      break;
    }

    success = (iter->second.secret == req.secret());
  } while (0);

  RAY_LOG(INFO) << "nid: " << req.node_id()
                << ", address: " << ep.to_string()
                << ", secret: " << req.secret()
                << ", failed_nodes_cnt: " << round_failed_nodes_.size()
                << ", try register " << (success ? "success!" : "failed!");

  reply.set_success(success);
}

void L1Failover::OnNodeFailed(const AbstractFailover::NodeContext &ctx) {
  if (!round_failed_nodes_.emplace(ctx.node_id).second) {
    return;
  }

  if (round_failed_nodes_.size() == 1) {
    OnRoundFailedBegin(ctx);
  }

  if (round_failed_nodes_.size() == registered_nodes_.size()) {
    OnRoundFailedEnd(ctx);
  }
}

void L1Failover::AcceptNode(const AbstractFailover::NodeContext &ctx) {
  RAY_LOG(INFO) << "nid: " << ctx.node_id << ", address: " << ctx.ep.to_string();
  registered_nodes_[ctx.node_id] = ctx;
}

void L1Failover::OnRoundFailedBegin(const AbstractFailover::NodeContext &ctx) {
  RAY_LOG(WARNING) << "**** Abnormal exit detected ****"
                   << " nid: " << ctx.node_id
                   << ", address: " << ctx.ep.to_string()
                   << ", registered_node_cnt: " << registered_nodes_.size();
  fd_->PauseOnMaster();

  for (auto &entry : registered_nodes_) {
    if (entry.first != ctx.node_id) {
      RAY_LOG(WARNING) << "reset nid: " << entry.second.node_id
                       << ", address: " << entry.second.ep.to_string()
                       << " as node(" << ctx.node_id << ", " << ctx.ep.to_string() << ")"
                       << " failed!";
      ResetNode(entry.second);
    }
  }
}

void L1Failover::OnRoundFailedEnd(const AbstractFailover::NodeContext &ctx) {
  registered_nodes_.clear();
  round_failed_nodes_.clear();
  fd_->ResumeOnMaster();
}

void L1Failover::ResetNode(const NodeContext &ctx) {
  // TODO(hc): Send reset command to node
}

}

}