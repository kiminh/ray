#include "task_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultTaskInfoHandler::HandleAddTask(const AddTaskRequest &request,
                                           AddTaskReply *reply,
                                           SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.task_data().task().task_spec().job_id());
  TaskID task_id = TaskID::FromBinary(request.task_data().task().task_spec().task_id());
  RAY_LOG(INFO) << "Adding task, task id = " << task_id << ", job id = " << job_id;
  auto task_table_data = std::make_shared<TaskTableData>();
  task_table_data->CopyFrom(request.task_data());
  auto on_done = [this, job_id, task_id, task_table_data, request,
                  send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task, task id = " << task_id
                     << ", job id = " << job_id;
    } else {
      RAY_LOG(INFO) << "Finished adding task, task id = " << task_id
                    << ", job id = " << job_id;
      RAY_CHECK_OK(task_pub_.Publish(task_id, *task_table_data,
                                     GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_table_storage_->TaskTable().Put(job_id, task_id, task_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultTaskInfoHandler::HandleGetTask(const GetTaskRequest &request,
                                           GetTaskReply *reply,
                                           SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_id());
  RAY_LOG(INFO) << "Getting task, task id = " << task_id;
  auto on_done = [task_id, request, reply, send_reply_callback](
                     Status status, const boost::optional<TaskTableData> &result) {
    if (status.ok()) {
      RAY_DCHECK(result);
      reply->mutable_task_data()->CopyFrom(*result);
      RAY_LOG(INFO) << "Finished getting task, task id = " << task_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get task, task id = " << task_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->TaskTable().Get(task_id.JobId(), task_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultTaskInfoHandler::HandleDeleteTasks(const DeleteTasksRequest &request,
                                               DeleteTasksReply *reply,
                                               SendReplyCallback send_reply_callback) {
  std::vector<TaskID> task_ids = IdVectorFromProtobuf<TaskID>(request.task_id_list());
  RAY_LOG(INFO) << "Deleting tasks, task id list size = " << task_ids.size();

  // Get task info.
  std::promise<bool> promise;
  std::unordered_map<TaskID, TaskTableData> tasks;
  for (auto &task_id : task_ids) {
    auto get_on_done = [&promise, &tasks, task_id, task_ids](
                           Status status, const boost::optional<TaskTableData> &result) {
      if (status.ok()) {
        RAY_DCHECK(result);
        tasks[task_id] = *result;
      } else {
        RAY_LOG(INFO) << "Get failed************************";
      }

      if (tasks.size() == task_ids.size()) {
        promise.set_value(true);
      }
    };
    Status status =
        gcs_table_storage_->TaskTable().Get(task_id.JobId(), task_id, get_on_done);
  }
  promise.get_future().get();

  auto on_done = [this, tasks, task_ids, request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to delete tasks, task id list size = " << task_ids.size();
    } else {
      RAY_LOG(INFO) << "Finished deleting tasks, task id list size = " << task_ids.size();
      for (auto &task : tasks) {
        RAY_CHECK_OK(
            task_pub_.Publish(task.first, task.second, GcsChangeMode::REMOVE, nullptr));
      }
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  JobID job_id = task_ids[0].JobId();
  Status status = gcs_table_storage_->TaskTable().Delete(job_id, task_ids, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultTaskInfoHandler::HandleAddTaskLease(const AddTaskLeaseRequest &request,
                                                AddTaskLeaseReply *reply,
                                                SendReplyCallback send_reply_callback) {
  TaskID task_id = TaskID::FromBinary(request.task_lease_data().task_id());
  ClientID node_id = ClientID::FromBinary(request.task_lease_data().node_manager_id());
  RAY_LOG(DEBUG) << "Adding task lease, task id = " << task_id
                 << ", node id = " << node_id;
  auto task_lease_data = std::make_shared<TaskLeaseData>();
  task_lease_data->CopyFrom(request.task_lease_data());
  auto on_done = [this, task_id, node_id, task_lease_data, request,
                  send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to add task lease, task id = " << task_id
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(DEBUG) << "Finished adding task lease, task id = " << task_id
                     << ", node id = " << node_id;
      RAY_CHECK_OK(task_lease_pub_.Publish(task_id, *task_lease_data,
                                           GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->TaskLeaseTable().Put(task_id.JobId(), task_id,
                                                           task_lease_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultTaskInfoHandler::HandleAttemptTaskReconstruction(
    const AttemptTaskReconstructionRequest &request,
    AttemptTaskReconstructionReply *reply, SendReplyCallback send_reply_callback) {
  ClientID node_id =
      ClientID::FromBinary(request.task_reconstruction().node_manager_id());
  RAY_LOG(DEBUG) << "Reconstructing task, reconstructions num = "
                 << request.task_reconstruction().num_reconstructions()
                 << ", node id = " << node_id;
  auto task_reconstruction_data = std::make_shared<TaskReconstructionData>();
  task_reconstruction_data->CopyFrom(request.task_reconstruction());
  auto on_done = [node_id, request, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to reconstruct task, reconstructions num = "
                     << request.task_reconstruction().num_reconstructions()
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(DEBUG) << "Finished reconstructing task, reconstructions num = "
                     << request.task_reconstruction().num_reconstructions()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  TaskID task_id = TaskID::FromBinary(request.task_reconstruction().task_id());
  Status status = gcs_table_storage_->TaskReconstructionTable().Put(
      task_id.JobId(), task_id, task_reconstruction_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace rpc
}  // namespace ray
