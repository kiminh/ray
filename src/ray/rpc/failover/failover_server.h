#pragma once

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "ray/rpc/asio_server.h"
#include "src/ray/protobuf/failover.pb.h"

namespace ray {
namespace rpc {
class FailoverHandler {
 public:
  virtual void HandleResetState(const ResetStateRequest &request, ResetStateReply *reply,
                                SendReplyCallback send_reply_callback) = 0;
};

/// The `AsioRpcService` for `FailoverService`.
class FailoverAsioRpcService : public AsioRpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit FailoverAsioRpcService(FailoverHandler &handler)
      : AsioRpcService(rpc::RpcServiceType::FailoverServiceType),
        service_handler_(handler){};

 protected:
  void InitMethodHandlers(
      std::vector<std::shared_ptr<ServiceMethod>> *server_call_methods,
      std::vector<std::string> *message_type_enum_names) override {
    // Initialize the Factory for `ResetState` requests.
    std::shared_ptr<ServiceMethod> push_task_call_method(
        new ServiceMethodImpl<FailoverHandler, ResetStateRequest, ResetStateReply,
                              FailoverMessageType>(
            service_type_, FailoverMessageType::ResetStateRequestMessage,
            FailoverMessageType::ResetStateReplyMessage, service_handler_,
            &FailoverHandler::HandleResetState));

    server_call_methods->emplace_back(std::move(push_task_call_method));

    *message_type_enum_names = GenerateEnumNames(FailoverMessageType);
  }

 private:
  /// The service handler that actually handle the requests.
  FailoverHandler &service_handler_;
};
}  // namespace rpc
}  // namespace ray