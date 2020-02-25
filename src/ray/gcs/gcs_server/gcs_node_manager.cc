#include "gcs_node_manager.h"
#include <ray/common/ray_config.h>
#include <ray/gcs/pb_util.h>
#include <ray/rpc/node_manager/node_manager_client.h>
#include <ray/rpc/worker/core_worker_client.h>
#include "gcs_actor_manager.h"
#include "gcs_leased_worker.h"
#include "ray/gcs/redis_gcs_client.h"

namespace ray {
namespace gcs {
std::shared_ptr<rpc::NodeManagerWorkerClient> GcsNode::GetOrCreateClient() {
  if (client_ == nullptr) {
    auto address = endpoint_.address().to_string();
    auto port = endpoint_.port();
    client_ = rpc::NodeManagerWorkerClient::make(address, port, client_call_manager_);
  }
  return client_;
}

std::unordered_map<WorkerID, std::shared_ptr<GcsLeasedWorker>>
GcsNode::RemoveAllLeasedWorkers() {
  auto leased_workers = std::move(leased_workers_);
  return leased_workers;
}

std::shared_ptr<GcsLeasedWorker> GcsNode::RemoveLeasedWorker(const WorkerID &worker_id) {
  std::shared_ptr<GcsLeasedWorker> leased_worker = nullptr;
  auto iter = leased_workers_.find(worker_id);
  if (iter != leased_workers_.end()) {
    leased_worker = iter->second;
    leased_workers_.erase(iter);
  }
  return leased_worker;
}

std::shared_ptr<GcsLeasedWorker> GcsNode::GetLeasedWorker(const WorkerID &worker_id) {
  std::shared_ptr<GcsLeasedWorker> leased_worker = nullptr;
  auto iter = leased_workers_.find(worker_id);
  if (iter != leased_workers_.end()) {
    leased_worker = iter->second;
  }
  return leased_worker;
}

std::unordered_map<ActorID, std::shared_ptr<GcsActor>> GcsNode::GetAllActors() const {
  std::unordered_map<ActorID, std::shared_ptr<GcsActor>> actors;
  for (auto &entry : leased_workers_) {
    if (auto actor = entry.second->GetActor()) {
      actors.emplace(actor->GetActorID(), std::move(actor));
    }
  }
  return actors;
}

void GcsNode::LeaseWorker(std::shared_ptr<GcsActor> actor,
                          std::function<void(const Status &, const ClientID &,
                                             std::shared_ptr<GcsLeasedWorker>)> &&cb) {
  RAY_CHECK(cb);
  rpc::RequestWorkerLeaseRequest request;
  request.mutable_resource_spec()->CopyFrom(actor->GetTaskSpecification().GetMessage());

  std::weak_ptr<GcsNode> weak_this(shared_from_this());
  auto client = GetOrCreateClient();
  auto status = client->RequestWorkerLease(
      request,
      [weak_this, cb](const Status &status, const rpc::RequestWorkerLeaseReply &reply) {
        if (auto this_node = weak_this.lock()) {
          if (!status.ok()) {
            // Session is broken.
            cb(status, ClientID::Nil(), nullptr);
            return;
          }

          const auto &retry_at_raylet_address = reply.retry_at_raylet_address();
          const auto &worker_address = reply.worker_address();
          if (worker_address.raylet_id().empty()) {
            RAY_CHECK(!retry_at_raylet_address.raylet_id().empty());
            auto node_id = ClientID::FromBinary(retry_at_raylet_address.raylet_id());
            cb(status, node_id, nullptr);
            return;
          }

          RAY_CHECK(worker_address.raylet_id() == this_node->GetNodeId().Binary());
          auto leased_worker = std::make_shared<GcsLeasedWorker>(
              worker_address, reply.resource_mapping(), this_node->client_call_manager_);
          this_node->leased_workers_.emplace(leased_worker->GetWorkerID(), leased_worker);
          cb(status, ClientID::Nil(), leased_worker);
        }
      });

  if (!status.ok()) {
    cb(status, ClientID::Nil(), nullptr);
  }
}

/////////////////////////////////////////////////////////////////////////////////////////
GcsNodeManager::GcsNodeManager(boost::asio::io_service &io_service,
                               std::shared_ptr<gcs::RedisGcsClient> gcs_client)
    : client_call_manager_(io_service),
      gcs_client_(std::move(gcs_client)),
      num_heartbeats_timeout_(RayConfig::instance().num_heartbeats_timeout()),
      heartbeat_timer_(io_service) {}

void GcsNodeManager::HandleHeartbeat(const ClientID &node_id,
                                     const rpc::HeartbeatTableData &heartbeat_data) {
  heartbeats_[node_id] = num_heartbeats_timeout_;
  heartbeat_buffer_[node_id] = heartbeat_data;
}

void GcsNodeManager::Start() {
  const auto heartbeat_callback = [this](const ClientID &id,
                                         const rpc::HeartbeatTableData &heartbeat_data) {
    HandleHeartbeat(id, heartbeat_data);
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncSubscribeHeartbeat(heartbeat_callback, nullptr));

  const auto lookup_callback = [this](Status status,
                                      const std::vector<GcsNodeInfo> &node_info_list) {
    for (const auto &node_info : node_info_list) {
      if (node_info.state() != rpc::GcsNodeInfo::DEAD) {
        // If there're any existing alive clients in client table, add them to
        // our `heartbeats_` cache. Thus, if they died before monitor starts,
        // we can also detect their death.
        // Use `emplace` instead of `operator []` because we just want to add this
        // client to `heartbeats_` only if it has not yet received heartbeat event.
        // Besides, it is not necessary to add an empty `HeartbeatTableData`
        // to `heartbeat_buffer_` as it doesn't make sense to broadcast an empty
        // message to the cluster and it's ok to add it when actually receive
        // its heartbeat event.
        heartbeats_.emplace(ClientID::FromBinary(node_info.node_id()),
                            num_heartbeats_timeout_);
        // Add node to local cache.
        if (auto node = AddNode(node_info)) {
          RAY_LOG(INFO) << "Succeed in loading node " << node->GetNodeId()
                        << " with endpoint " << node->GetEndpoint().to_string();
        }
      }
    }
    Tick();
  };
  RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(lookup_callback));
}

/// A periodic timer that checks for timed out clients.
void GcsNodeManager::Tick() {
  DetectDeadClients();
  SendBatchedHeartbeat();
  ScheduleTick();
}

void GcsNodeManager::DetectDeadClients() {
  for (auto it = heartbeats_.begin(); it != heartbeats_.end();) {
    it->second = it->second - 1;
    if (it->second == 0) {
      if (dead_nodes_.count(it->first) == 0) {
        auto client_id = it->first;
        RAY_LOG(WARNING) << "Client timed out: " << client_id;
        auto lookup_callback =
            [this, client_id](Status status, const std::vector<GcsNodeInfo> &all_node) {
              RAY_CHECK_OK(status);
              bool marked = false;
              for (const auto &node : all_node) {
                if (client_id.Binary() == node.node_id() &&
                    node.state() == GcsNodeInfo::DEAD) {
                  // The node has been marked dead by itself.
                  marked = true;
                  break;
                }
              }
              if (!marked) {
                if (auto node = RemoveNode(client_id)) {
                  if (node_failure_handler_) {
                    node_failure_handler_(std::move(node));
                  }
                }
                RAY_CHECK_OK(gcs_client_->Nodes().AsyncUnregister(client_id, nullptr));
                // Broadcast a warning to all of the drivers indicating that the node
                // has been marked as dead.
                // TODO(rkn): Define this constant somewhere else.
                std::string type = "node_removed";
                std::ostringstream error_message;
                error_message << "The node with client ID " << client_id
                              << " has been marked dead because the monitor"
                              << " has missed too many heartbeats from it.";
                auto error_data_ptr = gcs::CreateErrorTableData(type, error_message.str(),
                                                                current_time_ms());
                RAY_CHECK_OK(
                    gcs_client_->Errors().AsyncReportJobError(error_data_ptr, nullptr));
              }
            };
        RAY_CHECK_OK(gcs_client_->Nodes().AsyncGetAll(lookup_callback));
        dead_nodes_.insert(client_id);
      }
      it = heartbeats_.erase(it);
    } else {
      it++;
    }
  }
}

void GcsNodeManager::SendBatchedHeartbeat() {
  if (!heartbeat_buffer_.empty()) {
    auto batch = std::make_shared<HeartbeatBatchTableData>();
    for (const auto &heartbeat : heartbeat_buffer_) {
      batch->add_batch()->CopyFrom(heartbeat.second);
    }
    RAY_CHECK_OK(gcs_client_->Nodes().AsyncReportBatchHeartbeat(batch, nullptr));
    heartbeat_buffer_.clear();
  }
}

void GcsNodeManager::ScheduleTick() {
  auto heartbeat_period = boost::posix_time::milliseconds(
      RayConfig::instance().raylet_heartbeat_timeout_milliseconds());
  heartbeat_timer_.expires_from_now(heartbeat_period);
  heartbeat_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `heartbeat_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Checking heartbeat failed with error: " << error.message();
    Tick();
  });
}

std::shared_ptr<GcsNode> GcsNodeManager::GetNode(const ray::ClientID &node_id) const {
  auto iter = alive_nodes_.find(node_id);
  if (iter == alive_nodes_.end()) {
    return nullptr;
  }

  return iter->second;
}

const std::unordered_map<ClientID, std::shared_ptr<GcsNode>>
    &GcsNodeManager::GetAllAliveNodes() const {
  return alive_nodes_;
}

std::shared_ptr<GcsNode> GcsNodeManager::AddNode(const rpc::GcsNodeInfo &node_info) {
  auto node_id = ClientID::FromBinary(node_info.node_id());
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    return nullptr;
  }

  boost::asio::ip::detail::endpoint endpoint(
      boost::asio::ip::address::from_string(node_info.node_manager_address()),
      node_info.node_manager_port());
  auto node = std::make_shared<GcsNode>(node_id, endpoint, client_call_manager_);
  alive_nodes_.emplace(node_id, node);
  return node;
}

std::shared_ptr<GcsNode> GcsNodeManager::RemoveNode(const ray::ClientID &node_id) {
  std::shared_ptr<GcsNode> node = nullptr;
  auto iter = alive_nodes_.find(node_id);
  if (iter != alive_nodes_.end()) {
    node = iter->second;
    alive_nodes_.erase(iter);
  }
  return node;
}

}  // namespace gcs
}  // namespace ray
