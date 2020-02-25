#ifndef RAY_GCS_NODE_MANAGER_H
#define RAY_GCS_NODE_MANAGER_H

#include <ray/common/id.h>
#include <ray/protobuf/gcs.pb.h>
#include <ray/rpc/client_call.h>

namespace ray {

namespace rpc {
class NodeManagerWorkerClient;
}

namespace gcs {
class GcsActor;
class GcsLeasedWorker;
class GcsNode : public std::enable_shared_from_this<GcsNode> {
 public:
  explicit GcsNode(const ClientID &client_id,
                   const boost::asio::ip::detail::endpoint &endpoint,
                   rpc::ClientCallManager &client_call_manager)
      : client_id_(client_id),
        endpoint_(endpoint),
        client_call_manager_(client_call_manager) {}

  virtual ~GcsNode() = default;

  virtual void LeaseWorker(
      std::shared_ptr<GcsActor> actor,
      std::function<void(const Status &status, const ClientID &,
                         std::shared_ptr<GcsLeasedWorker> worker)> &&cb);

  ClientID GetNodeId() const { return client_id_; }
  const boost::asio::ip::detail::endpoint &GetEndpoint() const { return endpoint_; }

  std::unordered_map<WorkerID, std::shared_ptr<GcsLeasedWorker>> RemoveAllLeasedWorkers();
  std::shared_ptr<GcsLeasedWorker> RemoveLeasedWorker(const WorkerID &worker_id);
  std::shared_ptr<GcsLeasedWorker> GetLeasedWorker(const WorkerID &worker_id);
  std::unordered_map<ActorID, std::shared_ptr<GcsActor>> GetAllActors() const;

 protected:
  std::shared_ptr<rpc::NodeManagerWorkerClient> GetOrCreateClient();

 protected:
  ClientID client_id_;
  boost::asio::ip::detail::endpoint endpoint_;
  rpc::ClientCallManager &client_call_manager_;
  std::shared_ptr<rpc::NodeManagerWorkerClient> client_;
  std::unordered_map<WorkerID, std::shared_ptr<GcsLeasedWorker>> leased_workers_;
};

class RedisGcsClient;
class GcsNodeManager {
 public:
  explicit GcsNodeManager(boost::asio::io_service &io_service,
                          std::shared_ptr<gcs::RedisGcsClient> gcs_client);

  /// Listen for heartbeats from Raylets and mark Raylets
  /// that do not send a heartbeat within a given period as dead.
  void Start();

  /// Get node by node id.
  ///
  /// \param node_info The info fo the node.
  /// \return the added gcs node if node_id is not exist, else nullptr.
  std::shared_ptr<GcsNode> AddNode(const rpc::GcsNodeInfo &node_info);

  /// Remove Node from cache.
  ///
  /// \param node_id The id of the node.
  /// \return the removed gcs node if exist, else nullptr.
  std::shared_ptr<GcsNode> RemoveNode(const ClientID &node_id);

  /// Get node by node id.
  ///
  /// \param node_id The id of the node.
  /// \return node The gcs node.
  std::shared_ptr<GcsNode> GetNode(const ClientID &node_id) const;

  /// Get all alive nodes.
  ///
  /// \return all alive nodes.
  const std::unordered_map<ClientID, std::shared_ptr<GcsNode>> &GetAllAliveNodes() const;

  void SetNodeFailureHandler(
      std::function<void(std::shared_ptr<GcsNode> &&node)> &&handler) {
    node_failure_handler_ = std::move(handler);
  }

 protected:
  /// Handle a heartbeat from a Raylet.
  ///
  /// \param client_id The client ID of the Raylet that sent the heartbeat.
  /// \param heartbeat_data The heartbeat sent by the client.
  void HandleHeartbeat(const ClientID &client_id,
                       const rpc::HeartbeatTableData &heartbeat_data);

  /// A periodic timer that fires on every heartbeat period. Raylets that have
  /// not sent a heartbeat within the last num_heartbeats_timeout ticks will be
  /// marked as dead in the client table.
  void Tick();

  /// Check that if any raylet is inactive due to no heartbeat for a period of time.
  /// If found any, mark it as dead.
  void DetectDeadClients();

  /// Send any buffered heartbeats as a single publish.
  void SendBatchedHeartbeat();

  /// Schedule another tick after a short time.
  void ScheduleTick();

 private:
  rpc::ClientCallManager client_call_manager_;
  /// Alive gcs nodes.
  std::unordered_map<ClientID, std::shared_ptr<GcsNode>> alive_nodes_;
  /// A client to the GCS, through which heartbeats are received.
  std::shared_ptr<gcs::RedisGcsClient> gcs_client_;
  /// The number of heartbeats that can be missed before a client is removed.
  int64_t num_heartbeats_timeout_;
  /// A timer that ticks every heartbeat_timeout_ms_ milliseconds.
  boost::asio::deadline_timer heartbeat_timer_;
  /// For each Raylet that we receive a heartbeat from, the number of ticks
  /// that may pass before the Raylet will be declared dead.
  std::unordered_map<ClientID, int64_t> heartbeats_;
  /// The Raylets that have been marked as dead in gcs.
  std::unordered_set<ClientID> dead_nodes_;
  /// A buffer containing heartbeats received from node managers in the last tick.
  std::unordered_map<ClientID, rpc::HeartbeatTableData> heartbeat_buffer_;
  std::function<void(std::shared_ptr<GcsNode> &&node)> node_failure_handler_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_NODE_MANAGER_H
