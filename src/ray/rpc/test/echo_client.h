#ifndef RAY_RPC_ECHO_CLIENT_H
#define RAY_RPC_ECHO_CLIENT_H

#include <thread>

#include <grpcpp/grpcpp.h>

#include "ray/common/status.h"
#include "ray/rpc/grpc_client.h"
#include "ray/util/logging.h"
#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/protobuf/test.pb.h"

namespace ray {
namespace rpc {

/// Client used for communicating with a remote node manager server.
class EchoClient {
 public:
  /// Constructor.
  ///
  /// \param[in] address Address of the test server.
  /// \param[in] port Port of the node manager server.
  /// \param[in] client_call_manager The `ClientCallManager` used for managing requests.
  EchoClient(const std::string &address, const int port,
                  ClientCallManager &client_call_manager) {
    grpc_client_ = std::unique_ptr<GrpcClient<EchoService>>(
      new GrpcClient<EchoService>(address, port, client_call_manager));

    // CREATE_STREAM_RPC_CALL(EchoService, StreamEcho, callback, grpc_client_, echo_stream_)
  };

  /// Send an echo request.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  VOID_RPC_CLIENT_METHOD(EchoService, Echo, grpc_client_, )

  /// Send an echo request.
  ///
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  VOID_STREAM_RPC_CLIENT_METHOD(EchoService, StreamEcho, echo_stream_, )

/*
  Status Echo(const EchoRequest &request,
                   const ClientCallback<EchoReply> &callback) {
    auto call = client_call_manager_
                    .CreateCall<EchoService, EchoRequest, EchoReply>(
                        *stub_, &EchoService::Stub::PrepareAsyncEcho, request,
                        callback);
    return call->GetStatus();
  }
*/
  // Create the stream call and try to connect to the server synchronously.
  void StartStreamEcho(const ClientCallback<StreamEchoReply> &callback) {
    echo_stream_ =
        grpc_client_->
            CreateStream<StreamEchoRequest, StreamEchoReply>(
                &EchoService::Stub::AsyncStreamEcho, callback);
  }
/*
  /// Request for a stream message should be a synchronous call.
  void StreamEcho(const EchoRequest &request) {
    debug_stream_call_->WriteStream(request);
  }

  void CloseStreamEcho() { debug_stream_call_->WritesDone(); }
*/

 private:
  /// The RPC client.
  std::unique_ptr<GrpcClient<EchoService>> grpc_client_;

  std::shared_ptr<ClientStreamCallImpl<StreamEchoRequest, StreamEchoReply>> echo_stream_;
};

}  // namespace rpc
}  // namespace ray

#endif  // RAY_RPC_ECHO_CLIENT_H