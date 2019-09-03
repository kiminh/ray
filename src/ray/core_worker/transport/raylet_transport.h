#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>

#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

/// In raylet task submitter and receiver, a task is submitted to raylet, and possibly
/// gets forwarded to another raylet on which node the task should be executed, and
/// then a worker on that node gets this task and starts executing it.

class CoreWorkerRayletTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerRayletTaskSubmitter();

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpecification &task_spec) override;
};

class CoreWorkerRayletTaskReceiver : public CoreWorkerTaskReceiver,
                                     public rpc::WorkerTaskHandler {
 public:
  CoreWorkerRayletTaskReceiver(CoreWorkerStoreProviderMap &store_providers,
                               const TaskHandler &task_handler,
                               const WorkerServiceFinder &worker_service_finder);

  /// Handle a `AssignTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `send_reply_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] send_reply_callback The callback to be called when the request is done.
  void HandleAssignTask(const rpc::AssignTaskRequest &request,
                        std::shared_ptr<rpc::AssignTaskReply> reply,
                        rpc::SendReplyCallback send_reply_callback) override;

 private:
  // Object interface.
  CoreWorkerStoreProviderMap &store_providers_;
  /// The callback function to process a task.
  TaskHandler task_handler_;
  /// The callback to find the io_service to process tasks.
  WorkerServiceFinder worker_service_finder_;
};

class RayletGrpcTaskReceiver : public CoreWorkerRayletTaskReceiver {
 public:
  RayletGrpcTaskReceiver(CoreWorkerStoreProviderMap &store_providers,
                         boost::asio::io_service &io_service, rpc::GrpcServer &server,
                         const TaskHandler &task_handler,
                         const WorkerServiceFinder &worker_service_finder);

 private:
  /// The rpc service for `DirectActorService`.
  rpc::WorkerTaskGrpcService task_service_;
};

class RayletAsioTaskReceiver : public CoreWorkerRayletTaskReceiver {
 public:
  RayletAsioTaskReceiver(CoreWorkerStoreProviderMap &store_providers,
                         rpc::AsioRpcServer &server, const TaskHandler &task_handler,
                         const WorkerServiceFinder &worker_service_finder);

 private:
  /// The rpc service for `DirectActorService`.
  rpc::WorkerTaskAsioRpcService task_service_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
