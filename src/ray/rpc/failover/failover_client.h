#pragma once

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/asio_client.h"
#include "ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/failover.grpc.pb.h"
#include "src/ray/protobuf/failover.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a failover server.
class FailoverClient {
 public:
  /// Send reset state message.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status ResetState(const ResetStateRequest &request,
                                 const ClientCallback<ResetStateReply> &callback) = 0;
};

/// Client used for communicating with a failover server.
class FailoverAsioClient : public FailoverClient, public AsioRpcClient {
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
  FailoverAsioClient(const std::string &address, const int port,
                     boost::asio::io_service &io_service)
      : AsioRpcClient(RpcServiceType::FailoverServiceType, address, port, io_service) {}

  /// Reset state.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status ResetState(const ResetStateRequest &request,
                         const ClientCallback<ResetStateReply> &callback) override {
    return CallMethod<ResetStateRequest, ResetStateReply, FailoverMessageType>(
        FailoverMessageType::ResetStateRequestMessage,
        FailoverMessageType::ResetStateReplyMessage, request, callback);
  }
};

}  // namespace rpc
}  // namespace ray