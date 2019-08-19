#pragma once

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "ray/rpc/asio_server.h"
#include "src/ray/protobuf/gcsx.pb.h"

namespace ray {
namespace rpc {

class GcsServerHandler {
 public:
  virtual void HandleRegister(const RegisterRequest &request, RegisterReply *reply,
                              SendReplyCallback send_reply_callback) = 0;
};

/// The `AsioRpcService` for `GcsService`.
class GcsAsioRpcService : public AsioRpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] handler The service handler that actually handle the requests.
  explicit GcsAsioRpcService(GcsServerHandler &handler)
      : AsioRpcService(rpc::RpcServiceType::GcsServiceType), service_handler_(handler){};

 protected:
  void InitMethodHandlers(
      std::vector<std::shared_ptr<ServiceMethod>> *server_call_methods,
      std::vector<std::string> *message_type_enum_names) override {
    // Initialize the Factory for `Register` requests.
    std::shared_ptr<ServiceMethod> register_call_method(
        new ServiceMethodImpl<GcsServerHandler, RegisterRequest, RegisterReply,
                              GcsMessageType>(
            service_type_, GcsMessageType::RegisterRequestMessage,
            GcsMessageType::RegisterReplyMessage, service_handler_,
            &GcsServerHandler::HandleRegister));

    server_call_methods->emplace_back(std::move(register_call_method));

    *message_type_enum_names = GenerateEnumNames(GcsMessageType);
  }

 private:
  /// The service handler that actually handle the requests.
  GcsServerHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray