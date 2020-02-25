
#include "gcs_leased_worker.h"
#include <ray/common/ray_config.h>
#include <ray/rpc/worker/core_worker_client.h>

#include <utility>
#include "gcs_actor_manager.h"

namespace ray {
namespace gcs {
/////////////////////////////////////////////////////////////////////////////////////////
GcsLeasedWorker::GcsLeasedWorker(
    ray::rpc::Address address,
    const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &resources,
    rpc::ClientCallManager &client_call_manager)
    : address_(std::move(address)),
      resources_(resources),
      client_call_manager_(client_call_manager) {}

void GcsLeasedWorker::CreateActor(std::shared_ptr<GcsActor> actor,
                                  std::function<void(const Status &)> &&cb) {
  RAY_CHECK(cb);
  actor->SetWorker(shared_from_this());

  std::unique_ptr<rpc::PushTaskRequest> request(new rpc::PushTaskRequest());
  request->set_intended_worker_id(address_.worker_id());
  request->mutable_task_spec()->CopyFrom(actor->GetTaskSpecification().GetMessage());
  request->mutable_resource_mapping()->CopyFrom(resources_);

  auto client = GetOrCreateClient();
  auto status = client->PushNormalTask(
      std::move(request), [cb](Status status, const rpc::PushTaskReply &reply) {
        RAY_UNUSED(reply);
        cb(status);
      });
  if (!status.ok()) {
    cb(status);
  }
}

std::shared_ptr<GcsActor> GcsLeasedWorker::GetActor() const { return actor_; }

std::shared_ptr<GcsActor> GcsLeasedWorker::UnbindActor() {
  auto actor = std::move(actor_);
  if (actor != nullptr) {
    actor->ResetWorker();
  }
  return actor;
}

std::shared_ptr<rpc::CoreWorkerClient> GcsLeasedWorker::GetOrCreateClient() {
  if (client_ == nullptr) {
    rpc::Address address;
    address.set_ip_address(GetIpAddress());
    address.set_port(GetPort());
    client_ = std::make_shared<rpc::CoreWorkerClient>(address, client_call_manager_);
  }
  return client_;
}

}  // namespace gcs
}  // namespace ray
