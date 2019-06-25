#ifndef RAY_CORE_WORKER_RAYLET_TRANSPORT_H
#define RAY_CORE_WORKER_RAYLET_TRANSPORT_H

#include <list>

#include "ray/core_worker/transport/transport.h"
#include "ray/raylet/raylet_client.h"
#include "ray/rpc/worker_server.h"

namespace ray {

/// In raylet task submitter and receiver, a task is submitted to raylet, and possibly
/// gets forwarded to another raylet on which node the task should be executed, and
/// then a worker on that node gets this task and starts executing it.

class CoreWorkerRayletTaskSubmitter : public CoreWorkerTaskSubmitter {
 public:
  CoreWorkerRayletTaskSubmitter(std::unique_ptr<RayletClient> &raylet_client);

  /// Submit a task for execution to raylet.
  ///
  /// \param[in] task The task spec to submit.
  /// \return Status.
  virtual Status SubmitTask(const TaskSpec &task) override;

 private:
  /// Raylet client.
  std::unique_ptr<RayletClient> &raylet_client_;
};

class CoreWorkerRayletTaskReceiver
    : public CoreWorkerTaskReceiver,
      public rpc::WorkerTaskHandler {
 public:
  CoreWorkerRayletTaskReceiver(
    boost::asio::io_service &io_service,
    rpc::GrpcServer &server);

  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  void HandlePushTask(const rpc::PushTaskRequest &request,
                                 rpc::PushTaskReply *reply,
                                 rpc::RequestDoneCallback done_callback) override;

  Status SetTaskHandler(const TaskHandler &callback) override;                                 

 private:

  TaskHandler task_handler_;

  rpc::WorkerTaskGrpcService task_service_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_RAYLET_TRANSPORT_H
