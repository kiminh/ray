
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/common/task/task.h"
#include "ray/core_worker/core_worker.h"

using ray::rpc::ActorTableData;

namespace ray {

bool HasByReferenceArgs(const TaskSpecification &spec) {
  for (size_t i = 0; i < spec.NumArgs(); ++i) {
    if (spec.ArgIdCount(i) > 0) {
      return true;
    }
  }
  return false;
}

/*
 * CoreWorkerDirectActorTaskSubmitter
 */

CoreWorkerDirectActorTaskSubmitter::CoreWorkerDirectActorTaskSubmitter(
    gcs::RedisGcsClient &gcs_client,
    std::unique_ptr<CoreWorkerStoreProvider> store_provider)
    : gcs_client_(gcs_client), store_provider_(std::move(store_provider)) {
  RAY_CHECK_OK(SubscribeActorUpdates());
}

Status CoreWorkerDirectActorTaskSubmitter::SubmitTask(
    const TaskSpecification &task_spec) {
  if (HasByReferenceArgs(task_spec)) {
    return Status::Invalid("direct actor call only supports by-value arguments");
  }

  RAY_CHECK(task_spec.IsActorTask());
  const auto &actor_id = task_spec.ActorId();

  const auto task_id = task_spec.TaskId();
  const auto num_returns = task_spec.NumReturns();

  auto request = std::unique_ptr<rpc::PushTaskRequest>(new rpc::PushTaskRequest);
  request->mutable_task_spec()->Swap(&task_spec.GetMutableMessage());
  request->set_caller_worker_id(
      CoreWorkerProcess::GetCoreWorker()->GetWorkerID().Binary());

  std::unique_lock<std::mutex> guard(mutex_);
  auto iter = actor_states_.find(actor_id);
  if (iter == actor_states_.end() ||
      iter->second.state_ == ActorTableData::RECONSTRUCTING) {
    // Actor is not yet created, or is being reconstructed, cache the request
    // and submit after actor is alive.
    // TODO(zhijunfu): it might be possible for a user to specify an invalid
    // actor handle (e.g. from unpickling), in that case it might be desirable
    // to have a timeout to mark it as invalid if it doesn't show up in the
    // specified time.
    pending_requests_[actor_id].emplace_back(std::move(request));
    pending_tasks_[actor_id].insert(std::make_pair(task_id, num_returns));
    return Status::OK();
  } else if (iter->second.state_ == ActorTableData::ALIVE) {
    // Actor is alive, submit the request.
    if (rpc_clients_.count(actor_id) == 0) {
      // If rpc client is not available, then create it.
      ConnectAndSendPendingTasks(iter->second.worker_id_, actor_id,
                                 iter->second.location_.first,
                                 iter->second.location_.second);
    }

    // Submit request.
    auto &client = rpc_clients_[actor_id];
    PushTask(*client, *request, iter->second.worker_id_, actor_id, task_id, num_returns);
    return Status::OK();
  } else {
    // Actor is dead, treat the task as failure.
    RAY_CHECK(iter->second.state_ == ActorTableData::DEAD);
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
    return Status::IOError("Actor is dead.");
  }
}

Status CoreWorkerDirectActorTaskSubmitter::SubscribeActorUpdates() {
  // Register a callback to handle actor notifications.
  auto actor_notification_callback = [this](const ActorID &actor_id,
                                            const ActorTableData &actor_data) {
    const auto worker_id = WorkerID::FromBinary(actor_data.worker_id());
    std::unique_lock<std::mutex> guard(mutex_);
    actor_states_.erase(actor_id);
    actor_states_.emplace(
        actor_id, ActorStateData(actor_data.state(), worker_id, actor_data.ip_address(),
                                 actor_data.port()));

    if (actor_data.state() == ActorTableData::ALIVE) {
      // Check if this actor is the one that we're interested, if we already have
      // a connection to the actor, or have pending requests for it, we should
      // create a new connection.
      if (pending_requests_.count(actor_id) > 0) {
        ConnectAndSendPendingTasks(worker_id, actor_id, actor_data.ip_address(),
                                   actor_data.port());
      }
    } else {
      // Remove rpc client if it's dead or being reconstructed.
      rpc_clients_.erase(actor_id);
      // If this actor is permanently dead and there are pending requests, treat
      // the pending tasks as failed.
      if (actor_data.state() == ActorTableData::DEAD &&
          pending_requests_.count(actor_id) > 0) {
        for (const auto &request : pending_requests_[actor_id]) {
          TreatTaskAsFailed(TaskID::FromBinary(request->task_spec().task_id()),
                            request->task_spec().num_returns(),
                            rpc::ErrorType::ACTOR_DIED);
        }
        pending_requests_.erase(actor_id);
        pending_tasks_.erase(actor_id);
      }

      // For tasks that have been sent and are waiting for replies, treat them
      // as failed when the destination actor is dead or reconstructing.
      auto iter = waiting_reply_tasks_.find(actor_id);
      if (iter != waiting_reply_tasks_.end()) {
        for (const auto &entry : iter->second) {
          const auto &task_id = entry.first;
          const auto num_returns = entry.second;
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
        }
        waiting_reply_tasks_.erase(actor_id);
      }
    }

    RAY_LOG(INFO) << "received notification on actor, state="
                  << static_cast<int>(actor_data.state()) << ", actor_id: " << actor_id
                  << ", ip address: " << actor_data.ip_address()
                  << ", port: " << actor_data.port() << ", worker id: " << worker_id;
  };

  return gcs_client_.Actors().AsyncSubscribe(actor_notification_callback, nullptr);
}

void CoreWorkerDirectActorTaskSubmitter::ConnectAndSendPendingTasks(
    const WorkerID &callee_worker_id, const ActorID &actor_id, std::string ip_address,
    int port) {
  std::unique_ptr<rpc::DirectActorClient> grpc_client = CreateRpcClient(ip_address, port);
  RAY_CHECK(rpc_clients_.emplace(actor_id, std::move(grpc_client)).second);

  // Submit all pending requests.
  auto &client = rpc_clients_[actor_id];
  auto &requests = pending_requests_[actor_id];
  while (!requests.empty()) {
    auto &request = *requests.front();
    PushTask(*client, request, callee_worker_id, actor_id,
             TaskID::FromBinary(request.task_spec().task_id()),
             request.task_spec().num_returns());
    requests.pop_front();
  }

  pending_tasks_.erase(actor_id);
}

void CoreWorkerDirectActorTaskSubmitter::PushTask(rpc::DirectActorClient &client,
                                                  rpc::PushTaskRequest &request,
                                                  const WorkerID &callee_worker_id,
                                                  const ActorID &actor_id,
                                                  const TaskID &task_id,
                                                  int num_returns) {
  request.set_callee_worker_id(callee_worker_id.Binary());
  if (num_returns > 1) {
    waiting_reply_tasks_[actor_id].insert(std::make_pair(task_id, num_returns));
  }
  auto status =
      client.PushTask(request, [this, actor_id, task_id, num_returns](
                                   Status status, const rpc::PushTaskReply &reply) {
        if (!status.ok()) {
          TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
          // Remove the waiting reply task *after* the exception is written into store.
          waiting_reply_tasks_[actor_id].erase(task_id);
          return;
        }
        for (int i = 0; i < reply.return_objects_size(); i++) {
          const auto &return_object = reply.return_objects(i);
          ObjectID object_id = ObjectID::FromBinary(return_object.object_id());
          std::shared_ptr<LocalMemoryBuffer> data_buffer;
          if (return_object.data().size() > 0) {
            data_buffer = std::make_shared<LocalMemoryBuffer>(
                const_cast<uint8_t *>(
                    reinterpret_cast<const uint8_t *>(return_object.data().data())),
                return_object.data().size());
          }
          std::shared_ptr<LocalMemoryBuffer> metadata_buffer;
          if (return_object.metadata().size() > 0) {
            metadata_buffer = std::make_shared<LocalMemoryBuffer>(
                const_cast<uint8_t *>(
                    reinterpret_cast<const uint8_t *>(return_object.metadata().data())),
                return_object.metadata().size());
          }
          RAY_CHECK_OK(
              store_provider_->Put(RayObject(data_buffer, metadata_buffer), object_id));

          // Remove the waiting reply task *after* the object data is written into store.
          std::unique_lock<std::mutex> guard(mutex_);
          waiting_reply_tasks_[actor_id].erase(task_id);
        }
      });
  if (!status.ok()) {
    TreatTaskAsFailed(task_id, num_returns, rpc::ErrorType::ACTOR_DIED);
    // Remove the waiting reply task *after* the exception is written into store.
    waiting_reply_tasks_[actor_id].erase(task_id);
  }
}

void CoreWorkerDirectActorTaskSubmitter::TreatTaskAsFailed(
    const TaskID &task_id, int num_returns, const rpc::ErrorType &error_type) {
  for (int i = 0; i < num_returns; i++) {
    const auto object_id = ObjectID::ForTaskReturn(
        task_id, /*index=*/i + 1,
        /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT_ACTOR));
    std::string meta = std::to_string(static_cast<int>(error_type));
    auto metadata = const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
    auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size());
    RAY_CHECK_OK(store_provider_->Put(RayObject(nullptr, meta_buffer), object_id));
  }
}

bool CoreWorkerDirectActorTaskSubmitter::IsActorAlive(const ActorID &actor_id) const {
  std::unique_lock<std::mutex> guard(mutex_);
  auto iter = actor_states_.find(actor_id);
  return (iter != actor_states_.end() && iter->second.state_ == ActorTableData::ALIVE);
}

bool CoreWorkerDirectActorTaskSubmitter::ShouldWaitObjects(
    const std::vector<ObjectID> &object_ids) {
  for (const auto &object_id : object_ids) {
    bool task_finished = IsTaskFinished(object_id.TaskId());
    if (!task_finished) {
      return true;
    }
  }

  return false;
}

bool CoreWorkerDirectActorTaskSubmitter::IsTaskFinished(const TaskID &task_id) const {
  std::unique_lock<std::mutex> guard(mutex_);
  auto actor_id = task_id.ActorId();
  auto iter = pending_tasks_.find(actor_id);
  if (iter != pending_tasks_.end() && iter->second.count(task_id) > 0) {
    return false;
  }

  iter = waiting_reply_tasks_.find(actor_id);
  if (iter != waiting_reply_tasks_.end() && iter->second.count(task_id) > 0) {
    return false;
  }

  return true;
}

Status CoreWorkerDirectActorTaskSubmitter::GetReturnObjects(
    const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
    const TaskID &task_id,
    std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results) {
  if (object_ids.empty()) {
    return Status::OK();
  }

  std::unordered_set<ObjectID> unready(object_ids);

  int num_attempts = 0;
  bool should_break = false;
  int64_t remaining_timeout = timeout_ms;
  // Repeat until we get all objects.
  while (!unready.empty() && !should_break) {
    std::vector<ObjectID> unready_ids;
    for (const auto &entry : unready) {
      unready_ids.push_back(entry);
    }

    // Check whether we should wait for objects to be created/reconstructed,
    // or just fetch from store.
    bool should_wait = ShouldWaitObjects(unready_ids);

    // Get the objects from the object store, and parse the result.
    int64_t get_timeout = RayConfig::instance().get_timeout_milliseconds();
    if (!should_wait) {
      get_timeout = 0;
      remaining_timeout = 0;
      should_break = true;
    } else if (remaining_timeout >= 0) {
      get_timeout = std::min(remaining_timeout, get_timeout);
      remaining_timeout -= get_timeout;
      should_break = remaining_timeout <= 0;
    }

    std::vector<std::shared_ptr<RayObject>> result_objects;
    RAY_RETURN_NOT_OK(
        store_provider_->Get(unready_ids, get_timeout, task_id, &result_objects));

    for (size_t i = 0; i < result_objects.size(); i++) {
      if (result_objects[i] != nullptr) {
        const auto &object_id = unready_ids[i];
        (*results).emplace(object_id, result_objects[i]);
        unready.erase(object_id);
        if (result_objects[i]->IsException()) {
          should_break = true;
        }
      }
    }

    num_attempts += 1;
    CoreWorkerStoreProvider::WarnIfAttemptedTooManyTimes(num_attempts, unready);

    if (!should_wait && !unready.empty()) {
      // If the tasks that created these objects have already finished, but we are still
      // not able to get some of the objects from store, it's likely that these objects
      // have been evicted from store, so them as unreconstructable.
      std::string meta =
          std::to_string(static_cast<int>(rpc::ErrorType::OBJECT_UNRECONSTRUCTABLE));
      auto metadata =
          const_cast<uint8_t *>(reinterpret_cast<const uint8_t *>(meta.data()));
      auto meta_buffer = std::make_shared<LocalMemoryBuffer>(metadata, meta.size(), true);
      auto object = std::make_shared<RayObject>(nullptr, meta_buffer);
      for (const auto &entry : unready) {
        (*results).emplace(entry, object);
      }
    }
  }

  return Status::OK();
}

/*
 * CoreWorkerDirectActorTaskReceiver
 */
CoreWorkerDirectActorTaskReceiver::CoreWorkerDirectActorTaskReceiver(
    const TaskHandler &task_handler, const WorkerServiceFinder &worker_service_finder)
    : task_handler_(task_handler), worker_service_finder_(worker_service_finder) {}

void CoreWorkerDirectActorTaskReceiver::HandlePushTask(
    const rpc::PushTaskRequest &request, std::shared_ptr<rpc::PushTaskReply> reply,
    rpc::SendReplyCallback send_reply_callback) {
  const TaskSpecification task_spec(request.task_spec());
  const auto caller_worker_id = WorkerID::FromBinary(request.caller_worker_id());
  const auto callee_worker_id = WorkerID::FromBinary(request.callee_worker_id());
  RAY_LOG(DEBUG) << "Received task " << task_spec.TaskId() << " for worker "
                 << callee_worker_id << " from worker " << caller_worker_id;
  RAY_CHECK(task_spec.IsActorCreationTask() || task_spec.IsActorTask());
  auto num_returns = task_spec.NumReturns();
  RAY_CHECK(num_returns > 0);
  // Decrease to account for the dummy object id.
  num_returns--;

  if (HasByReferenceArgs(task_spec)) {
    CallSendReplyCallback(
        Status::Invalid("direct actor call only supports by value arguments"),
        num_returns, send_reply_callback);
    return;
  }

  auto &worker_service = worker_service_finder_(callee_worker_id);
  worker_service.post([this, task_spec, num_returns, reply, send_reply_callback]() {
    std::vector<std::shared_ptr<RayObject>> results;
    auto status = task_handler_(task_spec, &results);
    RAY_CHECK(results.size() == num_returns) << results.size() << "  " << num_returns;

    for (size_t i = 0; i < results.size(); i++) {
      auto return_object = (*reply).add_return_objects();
      ObjectID id = ObjectID::ForTaskReturn(
          task_spec.TaskId(), /*index=*/i + 1,
          /*transport_type=*/static_cast<int>(TaskTransportType::DIRECT_ACTOR));
      return_object->set_object_id(id.Binary());
      const auto &result = results[i];
      if (result->GetData() != nullptr) {
        return_object->set_data(result->GetData()->Data(), result->GetData()->Size());
      }
      if (result->GetMetadata() != nullptr) {
        return_object->set_metadata(result->GetMetadata()->Data(),
                                    result->GetMetadata()->Size());
      }
    }

    CallSendReplyCallback(status, num_returns, send_reply_callback);
  });
}

DirectActorGrpcTaskReceiver::DirectActorGrpcTaskReceiver(
    boost::asio::io_service &io_service, rpc::GrpcServer &server,
    const TaskHandler &task_handler, const WorkerServiceFinder &worker_service_finder)
    : CoreWorkerDirectActorTaskReceiver(task_handler, worker_service_finder),
      task_service_(io_service, *this) {
  server.RegisterService(task_service_);
}

void DirectActorGrpcTaskReceiver::CallSendReplyCallback(
    Status status, int num_returns, rpc::SendReplyCallback send_reply_callback) {
  // For Grpc, we always invoke `send_reply_callback` as Grpc unary mode requires
  // the reply.
  send_reply_callback(status, nullptr, nullptr);
}

DirectActorAsioTaskReceiver::DirectActorAsioTaskReceiver(
    rpc::AsioRpcServer &server, const TaskHandler &task_handler,
    const WorkerServiceFinder &worker_service_finder)
    : CoreWorkerDirectActorTaskReceiver(task_handler, worker_service_finder),
      task_service_(*this) {
  server.RegisterService(task_service_);
}

void DirectActorAsioTaskReceiver::CallSendReplyCallback(
    Status status, int num_returns, rpc::SendReplyCallback send_reply_callback) {
  // For asio based RPC, we only send the reply when the caller requires it,
  // which is indicated by a non-zero `num_returns`.
  if (num_returns > 0) {
    send_reply_callback(status, nullptr, nullptr);
  }
}

}  // namespace ray
