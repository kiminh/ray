#include "worker_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultWorkerInfoHandler::HandleReportWorkerFailure(
    const ReportWorkerFailureRequest &request, ReportWorkerFailureReply *reply,
    SendReplyCallback send_reply_callback) {
  Address worker_address = request.worker_failure().worker_address();
  RAY_LOG(DEBUG) << "Reporting worker failure, " << worker_address.DebugString();
  auto worker_failure_data = std::make_shared<WorkerFailureData>();
  worker_failure_data->CopyFrom(request.worker_failure());
  WorkerID worker_id =
      WorkerID::FromBinary(worker_failure_data->worker_address().worker_id());

  auto on_done = [this, worker_address, worker_id, worker_failure_data, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report worker failure, "
                     << worker_address.DebugString();
    } else {
      RAY_LOG(DEBUG) << "Finished reporting worker failure, "
                     << worker_address.DebugString();
      RAY_CHECK_OK(worker_failure_pub_.Publish(worker_id, *worker_failure_data,
                                               GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->WorkerFailureTable().Put(
      JobID::Nil(), worker_id, worker_failure_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace rpc
}  // namespace ray
