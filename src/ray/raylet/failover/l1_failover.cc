#include "l1_failover.h"
#include "ray/common/constants.h"

namespace ray {
namespace raylet {

void L1Failover::OnWorkerExit(Worker *worker) {
  RAY_LOG(ERROR) << "[" << __FUNCTION__ << "] "
                 << "pid: " << worker->Pid() << ", port: " << worker->Port()
                 << ", actor_id: " << worker->GetActorId()
                 << ", running_task_id: " << worker->GetAssignedTaskId();
  DoExit(kExitWorkerDisconnected);
}

void L1Failover::HandleResetState(const ray::rpc::ResetStateRequest &request,
                                  ray::rpc::ResetStateReply *reply,
                                  ray::rpc::SendReplyCallback send_reply_callback) {
  RAY_UNUSED(reply);
  RAY_UNUSED(send_reply_callback);
  RAY_LOG(ERROR) << "[" << __FUNCTION__ << "]";
  if (request.secret() != GetSecret()) {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] Ignore this reset."
                     << "L(" << GetSecret() << ") != R(" << request.secret() << ")";
    return;
  }

  DoExit(kExitReset);
}

void L1Failover::OnMasterDisconnected(const ip::detail::endpoint &endpoint) {
  RAY_LOG(ERROR) << "[" << __FUNCTION__ << "] "
                 << "endpoint: " << endpoint.to_string();
  DoExit(kExitMasterDisconnected);
}

void L1Failover::DoExit(int code) { exit(code); }

}  // namespace raylet
}  // namespace ray