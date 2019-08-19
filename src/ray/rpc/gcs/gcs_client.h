#pragma once

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/asio_client.h"
#include "ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/gcsx.grpc.pb.h"
#include "src/ray/protobuf/gcsx.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a gcs server.
class GcsClient {
 public:
  /// Send register message.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status Register(const RegisterRequest &request,
                               const ClientCallback<RegisterReply> &callback) = 0;
};

/// Client used for communicating with a failover server.
class GcsAsioClient : public GcsClient, public AsioRpcClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.

  /// Constructor.
  ///
  /// \param address Address of the failover server
  /// \param port Port of the failover server
  /// \param io_service Event engine the client will attach to
  GcsAsioClient(const std::string &address, const int port,
                boost::asio::io_service &io_service)
      : AsioRpcClient(RpcServiceType::GcsServiceType, address, port, io_service) {}

  /// Register.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status Register(const RegisterRequest &request,
                       const ClientCallback<RegisterReply> &callback) override {
    return CallMethod<RegisterRequest, RegisterReply, GcsMessageType>(
        GcsMessageType::RegisterRequestMessage, GcsMessageType::RegisterReplyMessage,
        request, callback);
  }
};

}  // namespace rpc
}  // namespace ray