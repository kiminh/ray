#ifndef RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
#define RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H

#include <list>

#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/worker/direct_actor_client.h"
#include "ray/rpc/worker/direct_actor_server.h"

namespace ray {

/// In direct actor call task submitter and receiver, a task is directly submitted
/// to the actor that will execute it.

/// The state data for an actor.
struct ActorStateData {
  ActorStateData(gcs::ActorTableData::ActorState state, const WorkerID worker_id,
                 const std::string &ip, int port)
      : state_(state), worker_id_(worker_id), location_(std::make_pair(ip, port)) {}

  /// Actor's state (e.g. alive, dead, reconstrucing).
  gcs::ActorTableData::ActorState state_;

  WorkerID worker_id_;

  /// IP address and port that the actor is listening on.
  std::pair<std::string, int> location_;
};

class CoreWorkerDirectActorTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerDirectActorTaskSubmitter(
      gcs::RedisGcsClient &gcs_client,
      std::unique_ptr<CoreWorkerStoreProvider> store_provider);

  /// Submit a task to an actor for execution.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  Status SubmitTask(const TaskSpecification &task_spec) override;

  /// Get a list of return objects for tasks submitted from this transport.
  ///
  /// \param[in] object_ids IDs of the objects to get.
  /// \param[in] timeout_ms Timeout in milliseconds, wait infinitely if it's -1.
  /// \param[in] task_id ID for the current task.
  /// \param[out] results Result list of objects data.
  /// \return Status.
  Status GetReturnObjects(
      const std::unordered_set<ObjectID> &object_ids, int64_t timeout_ms,
      const TaskID &task_id,
      std::unordered_map<ObjectID, std::shared_ptr<RayObject>> *results);

 protected:
  /// Create a RPC client to the specific address.
  ///
  /// \param[in] ip_address IP address of the server.
  /// \param[in] port Port that the server is listening on.
  /// \return Created RPC client.
  virtual std::unique_ptr<rpc::DirectActorClient> CreateRpcClient(std::string ip_address,
                                                                  int port) = 0;

 private:
  /// Subscribe to all actor updates.
  Status SubscribeActorUpdates();

  /// Push a task to a remote actor via the given client.
  /// Note, this function doesn't return any error status code. If an error occurs while
  /// sending the request, this task will be treated as failed.
  ///
  /// \param[in] client The RPC client to send tasks to an actor.
  /// \param[in] request The request to send.
  /// \param[in] callee_worker_id The ID of the worker which runs the actor.
  /// \param[in] task_id The ID of a task.
  /// \param[in] num_returns Number of return objects.
  /// \return Void.
  void PushTask(rpc::DirectActorClient &client, rpc::PushTaskRequest &request,
                const WorkerID &callee_worker_id, const ActorID &actor_id,
                const TaskID &task_id, int num_returns);

  /// Treat a task as failed.
  ///
  /// \param[in] task_id The ID of a task.
  /// \param[in] num_returns Number of return objects.
  /// \param[in] error_type The type of the specific error.
  /// \return Void.
  void TreatTaskAsFailed(const TaskID &task_id, int num_returns,
                         const rpc::ErrorType &error_type);

  /// Create connection to actor and send all pending tasks.
  /// Note that this function doesn't take lock, the caller is expected to hold
  /// `mutex_` before calling this function.
  ///
  /// \param[in] callee_worker_id TODO.
  /// \param[in] actor_id Actor ID.
  /// \param[in] ip_address The ip address of the node that the actor is running on.
  /// \param[in] port The port that the actor is listening on.
  /// \return Void.
  void ConnectAndSendPendingTasks(const WorkerID &callee_worker_id,
                                  const ActorID &actor_id, std::string ip_address,
                                  int port);

  /// Whether the specified actor is alive.
  ///
  /// \param[in] actor_id The actor ID.
  /// \return Whether this actor is alive.
  bool IsActorAlive(const ActorID &actor_id) const;

  /// Helper function to check whether to wait for a list of objects to appear,
  /// e.g. wait for the tasks which create these objects to finish.
  ///
  /// \param[in] object_ids A list of object ids.
  bool ShouldWaitObjects(const std::vector<ObjectID> &object_ids);

  /// Return if the task is finished.
  ///
  /// \param[in] task_id The ID of the task.
  /// \return Whether the task is finished.
  bool IsTaskFinished(const TaskID &task_id) const;

  /// Gcs client.
  gcs::RedisGcsClient &gcs_client_;

  /// Mutex to proect the various maps below.
  mutable std::mutex mutex_;

  /// Map from actor id to actor state. This currently includes all actors in the system.
  ///
  /// TODO(zhijunfu): this map currently keeps track of all the actors in the system,
  /// like `actor_registry_` in raylet. Later after new GCS client interface supports
  /// subscribing updates for a specific actor, this will be updated to only include
  /// entries for actors that the transport submits tasks to.
  std::unordered_map<ActorID, ActorStateData> actor_states_;

  /// Map from actor id to rpc client. This only includes actors that we send tasks to.
  ///
  /// TODO(zhijunfu): this will be moved into `actor_states_` later when we can
  /// subscribe updates for a specific actor.
  std::unordered_map<ActorID, std::unique_ptr<rpc::DirectActorClient>> rpc_clients_;

  /// Map from actor id to the actor's pending requests.
  std::unordered_map<ActorID, std::list<std::unique_ptr<rpc::PushTaskRequest>>>
      pending_requests_;

  /// Map from actor id to the tasks that are pending to send out.
  std::unordered_map<ActorID, std::unordered_map<TaskID, int>> pending_tasks_;
  /// Map from actor id to the tasks that are waiting for reply.
  std::unordered_map<ActorID, std::unordered_map<TaskID, int>> waiting_reply_tasks_;

  /// The store provider.
  std::unique_ptr<CoreWorkerStoreProvider> store_provider_;

  friend class CoreWorkerTest;
};

class DirectActorGrpcTaskSubmitter : public CoreWorkerDirectActorTaskSubmitter {
 public:
  DirectActorGrpcTaskSubmitter(boost::asio::io_service &io_service,
                               gcs::RedisGcsClient &gcs_client,
                               std::unique_ptr<CoreWorkerStoreProvider> store_provider)
      : CoreWorkerDirectActorTaskSubmitter(gcs_client, std::move(store_provider)),
        client_call_manager_(io_service) {}

  std::unique_ptr<rpc::DirectActorClient> CreateRpcClient(std::string ip_address,
                                                          int port) override {
    return std::unique_ptr<rpc::DirectActorGrpcClient>(
        new rpc::DirectActorGrpcClient(ip_address, port, client_call_manager_));
  }

 private:
  /// The `ClientCallManager` object that is shared by all `DirectActorClient`s.
  rpc::ClientCallManager client_call_manager_;
};

class DirectActorAsioTaskSubmitter : public CoreWorkerDirectActorTaskSubmitter {
 public:
  DirectActorAsioTaskSubmitter(boost::asio::io_service &io_service,
                               gcs::RedisGcsClient &gcs_client,
                               std::unique_ptr<CoreWorkerStoreProvider> store_provider)
      : CoreWorkerDirectActorTaskSubmitter(gcs_client, std::move(store_provider)),
        io_service_(io_service) {}

  std::unique_ptr<rpc::DirectActorClient> CreateRpcClient(std::string ip_address,
                                                          int port) override {
    return std::unique_ptr<rpc::DirectActorAsioClient>(
        new rpc::DirectActorAsioClient(ip_address, port, io_service_));
  }

 private:
  /// The IO event loop.
  boost::asio::io_service &io_service_;
};

class CoreWorkerDirectActorTaskReceiver : public CoreWorkerTaskReceiver,
                                          public rpc::DirectActorHandler {
 public:
  CoreWorkerDirectActorTaskReceiver(const TaskHandler &task_handler,
                                    const WorkerServiceFinder &worker_service_finder);

  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request,
                      std::shared_ptr<rpc::PushTaskReply> reply,
                      rpc::SendReplyCallback send_reply_callback) override;

 protected:
  /// Invoke the `send_reply_callback` for a task. This allows the derived class to decide
  /// whether to invoke the callback.
  ///
  /// \param[in] status Task execution status.
  /// \param[in] num_returns Number of return objects for the task.
  /// \param[out] send_reply_callback The reply callback for the task.
  /// \return Void.
  virtual void CallSendReplyCallback(Status status, int num_returns,
                                     rpc::SendReplyCallback send_reply_callback) = 0;

 private:
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The callback to find the io_service to process tasks.
  WorkerServiceFinder worker_service_finder_;
};

class DirectActorGrpcTaskReceiver : public CoreWorkerDirectActorTaskReceiver {
 public:
  DirectActorGrpcTaskReceiver(boost::asio::io_service &io_service,
                              rpc::GrpcServer &server, const TaskHandler &task_handler,
                              const WorkerServiceFinder &worker_service_finder);

 private:
  /// See base class for semantics.
  void CallSendReplyCallback(Status status, int num_returns,
                             rpc::SendReplyCallback send_reply_callback) override;

 private:
  /// The rpc service for `DirectActorService`.
  rpc::DirectActorGrpcService task_service_;
};

class DirectActorAsioTaskReceiver : public CoreWorkerDirectActorTaskReceiver {
 public:
  DirectActorAsioTaskReceiver(rpc::AsioRpcServer &server, const TaskHandler &task_handler,
                              const WorkerServiceFinder &worker_service_finder);

 private:
  /// See base class for semantics.
  void CallSendReplyCallback(Status status, int num_returns,
                             rpc::SendReplyCallback send_reply_callback) override;

  /// The rpc service for `DirectActorService`.
  rpc::DirectActorAsioRpcService task_service_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_DIRECT_ACTOR_TRANSPORT_H
