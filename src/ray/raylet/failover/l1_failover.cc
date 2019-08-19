#include "l1_failover.h"
#include "ray/common/constants.h"

namespace ray {
namespace raylet {

L1Failover::L1Failover(const std::shared_ptr<fd::FailureDetector> &fd) {
  fd->OnMasterDisconnected([this](const ip::detail::endpoint &endpoint) {
    this->OnMasterDisconnected(endpoint);
  });
}

void L1Failover::OnWorkerExit(Worker *worker) {
  RAY_LOG(ERROR) << "[" << __FUNCTION__ << "] "
                 << "pid: " << worker->Pid() << ", port: " << worker->Port()
                 << ", actor_id: " << worker->GetActorId()
                 << ", running_task_id: " << worker->GetAssignedTaskId();
  // TODO(hc): log the worker context
  DoExit(kExitWorkerDisconnected);
}

ray::Status L1Failover::OnResetState(const ray::rpc::ResetStateRequest &request,
                                     ray::rpc::ResetStateReply *reply) {
  RAY_UNUSED(reply);
  RAY_LOG(ERROR) << "[" << __FUNCTION__ << "]";
  DoExit(kExitReset);
  return ray::Status::OK();
}

void L1Failover::OnMasterDisconnected(const ip::detail::endpoint &endpoint) {
  RAY_LOG(ERROR) << "[" << __FUNCTION__ << "] "
                 << "endpoint: " << endpoint.to_string();
  DoExit(kExitMasterDisconnected);
}

void L1Failover::DoExit(int code) {
  // TODO(hc): flush all logs and exit
  // exit(code);
}

}  // namespace raylet
}  // namespace ray