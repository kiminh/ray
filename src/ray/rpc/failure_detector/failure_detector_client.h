#pragma once

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/asio_client.h"
#include "ray/rpc/client_call.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/failure_detector.grpc.pb.h"
#include "src/ray/protobuf/failure_detector.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a failure detector server.
class FailureDetectorClient {
 public:
  /// Send ping message.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  virtual ray::Status Ping(const BeaconMsg &request,
                           const ClientCallback<BeaconAck> &callback) = 0;
};

/// Client used for communicating with a failure detector server.
class FailureDetectorAsioClient : public FailureDetectorClient, public AsioRpcClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the direct actor server.
  /// \param[in] port Port of the direct actor server.
  /// \param[in] io_service Event engine the client will attach to.
  FailureDetectorAsioClient(const std::string &address, const int port,
                            boost::asio::io_service &io_service)
      : AsioRpcClient(RpcServiceType::FailureDetectorServiceType, address, port,
                      io_service) {}

  /// Send ping message.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  /// \return if the rpc call succeeds
  ray::Status Ping(const BeaconMsg &request,
                   const ClientCallback<BeaconAck> &callback) override {
    return CallMethod<BeaconMsg, BeaconAck, FailureDetectorMessageType>(
        FailureDetectorMessageType::BeaconRequestMessage,
        FailureDetectorMessageType::BeaconReplyMessage, request, callback);
  }
};

}  // namespace rpc
}  // namespace ray
