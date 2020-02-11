#ifndef RAY_RPC_ECHO_SERVER_H
#define RAY_RPC_ECHO_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/protobuf/test.pb.h"

namespace ray {
namespace rpc {

/// Interface of the `EchoServiceHandler`, see `src/ray/protobuf/test.proto`.
class EchoServiceHandler {
 public:
  /// Handle a `DebugEcho` request.
  virtual void HandleEcho(const EchoRequest &request, EchoReply *reply,
                          SendReplyCallback send_reply_callback) = 0;
  /// Handle `DebugStreamEcho` requests.
  virtual void HandleStreamEcho(
      const StreamEchoRequest &request,
      StreamEchoReply *reply,
      SendStreamReplyCallback send_reply_callback) = 0;
};

/// The `GrpcService` for `EchoService`.
class EchoGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] io_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  EchoGrpcService(boost::asio::io_service &io_service, EchoServiceHandler &service_handler)
      : GrpcService(io_service), service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the factory for `DebugEcho` requests.
    RPC_SERVICE_HANDLER(EchoService, Echo, 9999)

    STREAM_RPC_SERVICE_HANDLER(EchoService, StreamEcho, 10)
    /*
    // Initialize the factory for `DebugStreamEcho` requests.
    std::unique_ptr<ServerCallFactory> debug_stream_echo_call_factory(
        new ServerStreamCallFactoryImpl<EchoService, EchoServiceHandler,
                                        StreamEchoRequest, StreamEchoReply>(
            main_service_, cq, service_,
            &EchoService::AsyncService::RequestStreamEcho, service_handler_,
            &EchoServiceHandler::HandleStreamEcho));
    // Set `DebugStreamEcho`'s accept concurrency.
    server_call_factories_and_concurrencies->emplace_back(
        std::move(debug_stream_echo_call_factory), 1);
    */
  }

 private:
  /// The grpc async service object.
  EchoService::AsyncService service_;

  /// The service handler that actually handle the requests.
  EchoServiceHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif