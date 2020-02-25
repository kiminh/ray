
#include "gcs_actor_manager.h"
#include "gcs_actor_scheduling_strategy.h"
#include "gcs_leased_worker.h"
#include "gcs_node_manager.h"

namespace ray {
namespace gcs {

ClientID GcsActor::GetNodeID() const {
  return ClientID::FromBinary(actor_table_data_.address().raylet_id());
}

void GcsActor::SetNodeID(const ClientID &node_id) {
  rpc::Address address(actor_table_data_.address());
  address.set_raylet_id(node_id.Binary());
  actor_table_data_.mutable_address()->CopyFrom(address);
}

void GcsActor::ResetWorker() {
  worker_.reset();
  // update address
  rpc::Address address(actor_table_data_.address());
  address.clear_ip_address();
  address.clear_port();
  address.set_worker_id(WorkerID::Nil().Binary());
  actor_table_data_.mutable_address()->CopyFrom(address);
}

void GcsActor::SetWorker(std::weak_ptr<GcsLeasedWorker> &&worker) {
  worker_ = std::move(worker);
  auto wk = worker_.lock();
  RAY_CHECK(wk != nullptr);
  actor_table_data_.mutable_address()->CopyFrom(wk->GetAddress());
}

std::shared_ptr<GcsLeasedWorker> GcsActor::GetWorker() const { return worker_.lock(); }

WorkerID GcsActor::GetWorkerID() const {
  auto worker = GetWorker();
  return worker ? worker->GetWorkerID() : WorkerID::Nil();
}

void GcsActor::SetState(rpc::ActorTableData_ActorState state) {
  actor_table_data_.set_state(state);
}

rpc::ActorTableData_ActorState GcsActor::GetState() const {
  return actor_table_data_.state();
}

ActorID GcsActor::GetActorID() const {
  const auto &task_spec = actor_table_data_.task_spec();
  return ActorID::FromBinary(task_spec.actor_creation_task_spec().actor_id());
}

TaskSpecification GcsActor::GetTaskSpecification() const {
  const auto &task_spec = actor_table_data_.task_spec();
  return TaskSpecification(task_spec);
}

const rpc::ActorTableData &GcsActor::GetActorTableData() const {
  return actor_table_data_;
}

rpc::ActorTableData *GcsActor::GetMutableActorTableData() { return &actor_table_data_; }

/////////////////////////////////////////////////////////////////////////////////////////
GcsActorManager::GcsActorManager(boost::asio::io_context &io_context,
                                 std::shared_ptr<gcs::RedisGcsClient> gcs_client,
                                 std::weak_ptr<GcsNodeManager> &&gcs_node_manager)
    : io_context_(io_context),
      redis_gcs_client_(std::move(gcs_client)),
      gcs_node_manager_(std::move(gcs_node_manager)),
      gcs_actor_scheduling_strategy_(
          new GcsActorSchedulingStrategy(gcs_node_manager_.lock())) {
  StartSchedulingActors();
}

void GcsActorManager::StartSchedulingActors() {
  RAY_LOG(INFO) << "Start loading actors ...";
  auto actor_table_data_list = LoadAllActorTableData();
  for (auto &actor_table_data : actor_table_data_list) {
    auto actor = std::make_shared<GcsActor>(std::move(actor_table_data));
    actor->SetState(rpc::ActorTableData::PENDING);
    actors_.emplace(actor);
    actor_indexer_.emplace(actor->GetActorID(), actor);
  }
  RAY_LOG(INFO) << "Finished loading actors, size = " << actors_.size();
  io_context_.post([this] { DoScheduling(); });
}

void GcsActorManager::AsyncFlush(std::shared_ptr<GcsActor> actor,
                                 std::function<void()> &&callback) {
  RAY_CHECK(actor);
  auto actor_table_data = std::make_shared<rpc::ActorTableData>();
  actor_table_data->CopyFrom(actor->GetActorTableData());

  auto on_done = [actor, callback](Status status) {
    if (!status.ok()) {
      // Only one node at a time should succeed at creating or updating the actor.
      RAY_LOG(FATAL) << "Failed to update state to " << actor->GetState() << " for actor "
                     << actor->GetActorID();
    }
    if (callback) {
      callback();
    }
  };
  auto status = redis_gcs_client_->Actors().AsyncUpdate(actor->GetActorID(),
                                                        actor_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

std::vector<rpc::ActorTableData> GcsActorManager::LoadAllActorTableData() {
  std::vector<rpc::ActorTableData> actor_table_data_list;
  RAY_CHECK_OK(redis_gcs_client_->Actors().GetAll(&actor_table_data_list));
  return actor_table_data_list;
}

void GcsActorManager::DoScheduling() {
  RAY_LOG(DEBUG) << "Scheduling actor creation tasks, size = " << actors_.size();
  auto actors = std::move(actors_);
  while (!actors.empty()) {
    auto actor = actors.front();
    actors.pop();
    TryScheduling(actor);
  }

  auto schedule_period = boost::posix_time::milliseconds(3000);
  if (schedule_timer_ == nullptr) {
    schedule_timer_.reset(new boost::asio::deadline_timer(io_context_));
  }
  schedule_timer_->expires_from_now(schedule_period);
  schedule_timer_->async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      // `operation_canceled` is set when `heartbeat_timer_` is canceled or destroyed.
      // The Monitor lifetime may be short than the object who use it. (e.g. gcs_server)
      return;
    }
    RAY_CHECK(!error) << "Scheduling actor creation task failed with error: "
                      << error.message();
    DoScheduling();
  });
}

void GcsActorManager::TryScheduling(std::shared_ptr<GcsActor> actor) {
  auto node_id = actor->GetNodeID();
  if (!node_id.IsNil()) {
    if (auto gcs_node_manager = gcs_node_manager_.lock()) {
      auto node = gcs_node_manager->GetNode(node_id);
      if (node != nullptr) {
        LeaseWorker(actor, node);
        return;
      }
    }

    // The actor_table_data is just loaded from storage and the node id tied to
    // the actor represents a dead node, so we should reset the node id to nil and
    // select a new node to schedule the actor later.
    actor->SetNodeID(ClientID::Nil());
  }

  auto node = gcs_actor_scheduling_strategy_->SelectNode(actor);
  if (node == nullptr) {
    RAY_LOG(WARNING) << "Failed to create actor " << actor->GetActorID()
                     << " as there are no enough resources, enqueue the actor and "
                        "recreate it later.";
    Enqueue(actor);
    return;
  }

  // Update the node id so that the actor manager will select the same raylet to
  // lease worker for this actor when the previous leased worker dead.
  actor->SetNodeID(node->GetNodeId());
  std::weak_ptr<GcsNode> weak_node(node);
  AsyncFlush(actor, [this, actor, weak_node] {
    if (auto node = weak_node.lock()) {
      LeaseWorker(actor, node);
    } else {
      RAY_LOG(WARNING) << "Failed to create actor " << actor->GetActorID() << " as node "
                       << actor->GetNodeID()
                       << " is dead, enqueue the actor and recreate it later.";
      actor->SetNodeID(ClientID::Nil());
      // Since the actor is not yet bind with leased worker, so we should enqueue the
      // actor otherwise it will never be created later.
      Enqueue(actor);
    }
  });
}

void GcsActorManager::LeaseWorker(std::shared_ptr<GcsActor> actor,
                                  std::shared_ptr<GcsNode> node) {
  RAY_CHECK(actor && node);
  RAY_LOG(DEBUG) << "Start leasing worker from node " << node->GetNodeId()
                 << " for actor " << actor->GetActorID();

  std::weak_ptr<GcsNode> weak_node(node);
  node->LeaseWorker(actor, [this, actor, weak_node](
                               const Status &status, const ClientID &spillback_to,
                               std::shared_ptr<GcsLeasedWorker> worker) {
    if (auto node = weak_node.lock()) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Failed to lease worker from node " << node->GetNodeId()
                         << " for actor " << actor->GetActorID()
                         << " as the session between gcs server and node is broken.";
        // Unbind the actor to the node as it is not exist so that we can select
        // a new raylet in next round when recreate this actor.
        actor->SetNodeID(ClientID::Nil());
        OnActorFailure(actor);
        return;
      }

      if (!spillback_to.IsNil()) {
        RAY_CHECK(worker == nullptr);
        actor->SetNodeID(spillback_to);
        AsyncFlush(actor, [this, actor] { Enqueue(actor); });
        return;
      }

      RAY_LOG(INFO) << "Succeed in leasing worker " << worker->GetWorkerID() << " from "
                    << worker->GetNodeID() << " for actor " << actor->GetActorID();
      OnWorkerLeased(actor, worker);
    }
  });
}

void GcsActorManager::OnWorkerLeased(std::shared_ptr<GcsActor> actor,
                                     std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_CHECK(actor && worker && actor->GetWorker() == nullptr);
  RAY_LOG(INFO) << "Start creating actor " << actor->GetActorID() << " at "
                << worker->GetWorkerID() << " in node " << actor->GetNodeID();
  std::weak_ptr<GcsLeasedWorker> weak_worker(worker);
  worker->CreateActor(actor, [this, actor, weak_worker](const Status &status) {
    if (auto worker = weak_worker.lock()) {
      if (!status.ok()) {
        RAY_LOG(WARNING) << "Failed to create actor " << actor->GetActorID()
                         << " on worker " << worker->GetWorkerID() << " with node id "
                         << worker->GetNodeID() << ", address " << worker->GetIpAddress()
                         << ":" << worker->GetPort() << " as session is broken.";
        OnActorFailure(actor);
        return;
      }

      RAY_LOG(INFO) << "Succeed in creating actor " << actor->GetActorID() << " at "
                    << worker->GetWorkerID() << " in node " << actor->GetNodeID();
      OnActorCreateSuccess(actor, worker);
    }
  });
}

void GcsActorManager::OnActorFailure(std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor != nullptr);

  auto worker = actor->GetWorker();
  RAY_CHECK(worker != nullptr && actor == worker->UnbindActor());
  auto actor_table_data = actor->GetMutableActorTableData();
  auto remaining_reconstructions = actor_table_data->remaining_reconstructions();
  if (remaining_reconstructions > 0) {
    actor_table_data->set_remaining_reconstructions(--remaining_reconstructions);
    actor_table_data->set_state(rpc::ActorTableData::RECONSTRUCTING);
    AsyncFlush(actor, [this, actor] { Enqueue(std::move(actor)); });
  } else {
    actor_table_data->set_state(rpc::ActorTableData::DEAD);
    AsyncFlush(actor, nullptr);
  }
}

void GcsActorManager::OnActorCreateSuccess(std::shared_ptr<GcsActor> actor,
                                           std::shared_ptr<GcsLeasedWorker> worker) {
  RAY_CHECK(actor && worker && actor->GetWorker());
  actor->SetState(rpc::ActorTableData::ALIVE);

  //  auto actor_id = actor->GetActorID();
  //
  //  auto actor_table_data = std::make_shared<ActorTableData>();
  //  actor_table_data->CopyFrom(actor->GetActorTableData());
  //  actor_table_data->set_state(ActorTableData::ALIVE);

  AsyncFlush(actor, nullptr);
}

void GcsActorManager::Enqueue(std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor->GetWorker() == nullptr);
  io_context_.post([=] { actors_.push(actor); });
}

void GcsActorManager::CreateActor(const ray::rpc::CreateActorRequest &request,
                                  std::function<void(Status)> &&callback) {
  RAY_CHECK(callback);
  if (!request.task_spec().is_direct_call()) {
    callback(Status::Invalid("The field of is_direct_call in task_spec must be true."));
    return;
  }

  const auto &actor_creation_task_spec = request.task_spec().actor_creation_task_spec();
  auto actor_id = ActorID::FromBinary(actor_creation_task_spec.actor_id());
  if (actor_indexer_.count(actor_id) != 0) {
    // The actor is already exist, just ignore it.
    callback(Status::OK());
    return;
  }

  rpc::ActorTableData actor_table_data;
  actor_table_data.set_actor_id(actor_id.Binary());
  actor_table_data.set_job_id(request.task_spec().job_id());
  actor_table_data.set_max_reconstructions(
      actor_creation_task_spec.max_actor_reconstructions());
  actor_table_data.set_remaining_reconstructions(
      actor_creation_task_spec.max_actor_reconstructions());

  auto dummy_object = TaskSpecification(request.task_spec()).ActorDummyObject().Binary();
  actor_table_data.set_actor_creation_dummy_object_id(dummy_object);

  actor_table_data.set_is_direct_call(request.task_spec().is_direct_call());
  actor_table_data.set_is_detached(actor_creation_task_spec.is_detached());
  actor_table_data.mutable_owner_address()->CopyFrom(
      request.task_spec().caller_address());

  actor_table_data.set_state(ActorTableData::PENDING);
  actor_table_data.mutable_task_spec()->CopyFrom(request.task_spec());

  actor_table_data.mutable_address()->set_raylet_id(ClientID::Nil().Binary());
  actor_table_data.mutable_address()->set_worker_id(WorkerID::Nil().Binary());

  auto actor = std::make_shared<GcsActor>(std::move(actor_table_data));
  actor_indexer_.emplace(actor_id, actor);
  AsyncFlush(actor, [this, actor, callback] {
    // The actor is successfully persisted to the storage, so we could enqueue
    // and create this actor later and reply to the raylet.
    Enqueue(actor);
    callback(Status::OK());
  });
}

}  // namespace gcs
}  // namespace ray
