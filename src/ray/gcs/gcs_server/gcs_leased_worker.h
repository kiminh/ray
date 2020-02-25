#ifndef RAY_GCS_LEASED_WORKER_H
#define RAY_GCS_LEASED_WORKER_H

#include <ray/common/id.h>
#include <ray/protobuf/gcs.pb.h>
#include <ray/rpc/client_call.h>

namespace ray {

namespace rpc {
class CoreWorkerClient;
}

namespace gcs {
class GcsActor;
class GcsLeasedWorker : public std::enable_shared_from_this<GcsLeasedWorker> {
 public:
  explicit GcsLeasedWorker(
      rpc::Address address,
      const google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> &resources,
      rpc::ClientCallManager &client_call_manager);
  virtual ~GcsLeasedWorker() = default;

  virtual void CreateActor(std::shared_ptr<GcsActor> actor,
                           std::function<void(const Status &status)> &&cb);

  const rpc::Address &GetAddress() const { return address_; }
  const std::string &GetIpAddress() const { return address_.ip_address(); }
  uint16_t GetPort() const { return address_.port(); }
  WorkerID GetWorkerID() const { return WorkerID::FromBinary(address_.worker_id()); }
  ClientID GetNodeID() const { return ClientID::FromBinary(address_.raylet_id()); }

  std::shared_ptr<GcsActor> GetActor() const;
  std::shared_ptr<GcsActor> UnbindActor();

 private:
  std::shared_ptr<rpc::CoreWorkerClient> GetOrCreateClient();

 private:
  rpc::Address address_;
  google::protobuf::RepeatedPtrField<rpc::ResourceMapEntry> resources_;
  rpc::ClientCallManager &client_call_manager_;
  std::shared_ptr<GcsActor> actor_;
  std::shared_ptr<rpc::CoreWorkerClient> client_;
};
}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_LEASED_WORKER_H
