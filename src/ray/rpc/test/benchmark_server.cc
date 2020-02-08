#include <fstream>
#include <grpc/grpc.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>
#include <boost/asio.hpp>

#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/rpc/test/echo_server.h"

using grpc::Server;
using grpc::ServerAsyncReaderWriter;
using grpc::ServerBuilder;
using grpc::ServerCompletionQueue;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::Status;

using ray::rpc::EchoServiceHandler;
using ray::rpc::EchoRequest;
using ray::rpc::EchoReply;
using ray::rpc::StreamEchoRequest;
using ray::rpc::StreamEchoReply;
using ray::rpc::SendReplyCallback;
using ray::rpc::SendStreamReplyCallback;

using namespace std;

class BenchmarkServer : public ray::rpc::EchoServiceHandler {
public:
  BenchmarkServer(int port)
    : server_("benchmark_server", port),
      grpc_service_(io_service_, *this) {}

  ~BenchmarkServer() {
    io_service_.stop();
  }

  /// Handle a `DebugEcho` request.
  void HandleEcho(const EchoRequest &request, EchoReply *reply,
                  SendReplyCallback send_reply_callback) override {
    
    reply->set_reply_message(request.request_message());                
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);
  }

  /// Handle `DebugStreamEcho` requests.
  void HandleStreamEcho(
      const StreamEchoRequest &request,
      StreamEchoReply *reply,
      SendStreamReplyCallback send_reply_callback) override {
    
    cout << "server received request: " << request.request_id() << endl;
    reply->set_request_id(request.request_id());
    reply->set_reply_message(request.request_message());                
    send_reply_callback();

  }

  void Run() {
    server_.RegisterService(grpc_service_);
    server_.Run();

    boost::asio::io_service::work work(io_service_);
    io_service_.run();
  }

private:
  boost::asio::io_service io_service_;

  ray::rpc::GrpcServer server_;

    /// Common rpc service for all worker modules.
  ray::rpc::EchoGrpcService grpc_service_;
};

int main(int argc, char **argv) {
  int port = atoi(argv[1]);

  BenchmarkServer server(port);
  server.Run();
}