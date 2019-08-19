#pragma once

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "ray/rpc/asio_server.h"
#include "src/ray/protobuf/failure_detector.pb.h"

namespace ray {
namespace rpc {

class FailureDetectorHandler {
 public:
  virtual void HandlePing(const BeaconMsg &request, BeaconAck *reply,
                          SendReplyCallback send_reply_callback) = 0;
};

/// The `AsioRpcService` for `FailureDetecotrService`.
class FailureDetectorAsioRpcService : public AsioRpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit FailureDetectorAsioRpcService(FailureDetectorHandler &handler)
      : AsioRpcService(rpc::RpcServiceType::FailureDetectorServiceType),
        service_handler_(handler){};

 protected:
  void InitMethodHandlers(
      std::vector<std::shared_ptr<ServiceMethod>> *server_call_methods,
      std::vector<std::string> *message_type_enum_names) override {
    // Initialize the Factory for `Ping` requests.
    std::shared_ptr<ServiceMethod> push_task_call_method(
        new ServiceMethodImpl<FailureDetectorHandler, BeaconMsg, BeaconAck,
                              FailureDetectorMessageType>(
            service_type_, FailureDetectorMessageType::BeaconRequestMessage,
            FailureDetectorMessageType::BeaconReplyMessage, service_handler_,
            &FailureDetectorHandler::HandlePing));

    server_call_methods->emplace_back(std::move(push_task_call_method));

    *message_type_enum_names = GenerateEnumNames(FailureDetectorMessageType);
  }

 private:
  /// The service handler that actually handle the requests.
  FailureDetectorHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray