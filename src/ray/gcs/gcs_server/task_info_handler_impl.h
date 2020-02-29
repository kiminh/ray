#ifndef RAY_GCS_TASK_INFO_HANDLER_IMPL_H
#define RAY_GCS_TASK_INFO_HANDLER_IMPL_H

#include "ray/gcs/gcs_storage_client/gcs_storage_accessor.h"
#include "ray/gcs/gcs_storage_client/gcs_storage_client.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/rpc/gcs_server/gcs_rpc_server.h"

namespace ray {
namespace rpc {

/// This implementation class of `TaskInfoHandler`.
class DefaultTaskInfoHandler : public rpc::TaskInfoHandler {
 public:
  explicit DefaultTaskInfoHandler(gcs::GcsStorageClient &gcs_storage_client) {
    task_info_accessor_ = std::unique_ptr<gcs::GcsStorageTaskInfoAccessor>(
        new gcs::GcsStorageTaskInfoAccessor(gcs_storage_client));
  }

  void HandleAddTask(const AddTaskRequest &request, AddTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleGetTask(const GetTaskRequest &request, GetTaskReply *reply,
                     SendReplyCallback send_reply_callback) override;

  void HandleDeleteTasks(const DeleteTasksRequest &request, DeleteTasksReply *reply,
                         SendReplyCallback send_reply_callback) override;

  void HandleAddTaskLease(const AddTaskLeaseRequest &request, AddTaskLeaseReply *reply,
                          SendReplyCallback send_reply_callback) override;

  void HandleAttemptTaskReconstruction(const AttemptTaskReconstructionRequest &request,
                                       AttemptTaskReconstructionReply *reply,
                                       SendReplyCallback send_reply_callback) override;

 private:
  std::unique_ptr<gcs::GcsStorageTaskInfoAccessor> task_info_accessor_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_GCS_TASK_INFO_HANDLER_IMPL_H
