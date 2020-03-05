#include "error_info_handler_impl.h"

namespace ray {
namespace rpc {

void DefaultErrorInfoHandler::HandleReportJobError(
    const ReportJobErrorRequest &request, ReportJobErrorReply *reply,
    SendReplyCallback send_reply_callback) {
  JobID job_id = JobID::FromBinary(request.error_data().job_id());
  std::string type = request.error_data().type();
  RAY_LOG(DEBUG) << "Reporting job error, job id = " << job_id << ", type = " << type;
  auto error_table_data = std::make_shared<ErrorTableData>();
  error_table_data->CopyFrom(request.error_data());
  auto on_done = [job_id, type, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report job error, job id = " << job_id
                     << ", type = " << type;
    } else {
      RAY_LOG(DEBUG) << "Finished reporting job error, job id = " << job_id
                     << ", type = " << type;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_table_storage_->ErrorInfoTable().Put(job_id, job_id, error_table_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace rpc
}  // namespace ray
