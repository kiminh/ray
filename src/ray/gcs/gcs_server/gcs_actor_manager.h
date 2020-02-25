#ifndef RAY_GCS_ACTOR_MANAGER_H
#define RAY_GCS_ACTOR_MANAGER_H

#include <ray/common/id.h>
#include <ray/common/task/task_execution_spec.h>
#include <ray/common/task/task_spec.h>
#include <ray/gcs/redis_gcs_client.h>
#include <ray/protobuf/gcs.pb.h>
#include <boost/asio/deadline_timer.hpp>
#include <boost/asio/io_context.hpp>
#include <queue>

#include "gcs_actor_scheduling_strategy.h"

namespace ray {
namespace gcs {

class GcsLeasedWorker;
class GcsActor {
 public:
  explicit GcsActor(rpc::ActorTableData &&actor_table_data)
      : actor_table_data_(std::move(actor_table_data)) {}

  ClientID GetNodeID() const;
  void SetNodeID(const ClientID &node_id);

  void ResetWorker();
  void SetWorker(std::weak_ptr<GcsLeasedWorker> &&worker);
  std::shared_ptr<GcsLeasedWorker> GetWorker() const;
  WorkerID GetWorkerID() const;

  void SetState(rpc::ActorTableData_ActorState state);
  rpc::ActorTableData_ActorState GetState() const;

  ActorID GetActorID() const;
  TaskSpecification GetTaskSpecification() const;

  const rpc::ActorTableData &GetActorTableData() const;
  rpc::ActorTableData *GetMutableActorTableData();

 private:
  rpc::ActorTableData actor_table_data_;
  std::weak_ptr<GcsLeasedWorker> worker_;
};

class GcsNode;
class GcsNodeManager;
class GcsActorManager {
 public:
  explicit GcsActorManager(boost::asio::io_context &io_context,
                           std::shared_ptr<gcs::RedisGcsClient> gcs_client,
                           std::weak_ptr<GcsNodeManager> &&gcs_node_manager);
  ~GcsActorManager() = default;

  /// Start scheduling actors.
  void StartSchedulingActors();

  /// Create actor asynchronously
  void CreateActor(const rpc::CreateActorRequest &request,
                   std::function<void(Status)> &&callback);

  /// The handler to process the failure of actor. The failure is detected when worker or
  /// node associated with this actor is dead.
  void OnActorFailure(std::shared_ptr<GcsActor> actor);

 protected:
  void DoScheduling();

  /// Try scheduling an actor.
  void TryScheduling(std::shared_ptr<GcsActor> actor);

  /// Lease a worker from the specified node for the actor.
  void LeaseWorker(std::shared_ptr<GcsActor> actor, std::shared_ptr<GcsNode> node);

  /// Callback of `LeaseWorker`.
  void OnWorkerLeased(std::shared_ptr<GcsActor> actor,
                      std::shared_ptr<GcsLeasedWorker> worker);

  /// Callback of `CreateActor`.
  void OnActorCreateSuccess(std::shared_ptr<GcsActor> actor,
                            std::shared_ptr<GcsLeasedWorker> worker);

  /// Enqueue the specified actor and schedule later.
  void Enqueue(std::shared_ptr<GcsActor> actor);

  /// Flush the actor info to the storage asynchronously.
  void AsyncFlush(std::shared_ptr<GcsActor> actor, std::function<void()> &&callback);

  /// Load all actor info from storage.
  std::vector<rpc::ActorTableData> LoadAllActorTableData();

 private:
  /// The actor queue, all of the actors in the queue will be scheduled.
  std::queue<std::shared_ptr<GcsActor>> actors_;
  /// The indexer of actors.
  std::unordered_map<ActorID, std::shared_ptr<GcsActor>> actor_indexer_;

  boost::asio::io_context &io_context_;
  std::unique_ptr<boost::asio::deadline_timer> schedule_timer_;

  /// Redis gcs client.
  std::shared_ptr<gcs::RedisGcsClient> redis_gcs_client_;

  /// All available node.
  std::weak_ptr<GcsNodeManager> gcs_node_manager_;
  std::unique_ptr<GcsActorSchedulingStrategy> gcs_actor_scheduling_strategy_;
};

}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_ACTOR_MANAGER_H
