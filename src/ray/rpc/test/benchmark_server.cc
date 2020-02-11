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
      grpc_service_(io_service_, *this),
      start_(std::chrono::system_clock::now()),
      end_(std::chrono::system_clock::now()),
      count_(0)  {}

  ~BenchmarkServer() {
    io_service_.stop();
  }

  /// Handle a `DebugEcho` request.
  void HandleEcho(const EchoRequest &request, EchoReply *reply,
                  SendReplyCallback send_reply_callback) override {
    MayReportPerf();

    // cout << "server stream request: " << request.request_id() << endl;
    reply->set_request_id(request.request_id());
    reply->set_reply_message(request.request_message());                
    send_reply_callback(ray::Status::OK(), nullptr, nullptr);
  }

  /// Handle `DebugStreamEcho` requests.
  void HandleStreamEcho(
      const StreamEchoRequest &request,
      StreamEchoReply *reply,
      SendStreamReplyCallback send_reply_callback) override {
    MayReportPerf();

    // cout << "server stream request: " << request.request_id() << endl;
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
  void MayReportPerf() {
    count_ ++;
    auto batch_count = 10000;
    if (count_ % batch_count == 0) {
      end_ = std::chrono::system_clock::now();
      std::chrono::duration<double> diff = end_ - start_;
      // double gbps = 8.0 * onehm / diff.count() / 1e9;
      double gbps = batch_count / diff.count() / 1000;
      std::cout << gbps << " K, "
                << count_ << std::endl;
      start_ = end_;     
    }
  }

  boost::asio::io_service io_service_;

  ray::rpc::GrpcServer server_;

    /// Common rpc service for all worker modules.
  ray::rpc::EchoGrpcService grpc_service_;

  std::chrono::system_clock::time_point start_;
  std::chrono::system_clock::time_point end_;

  std::atomic<uint64_t> count_;
};

int main(int argc, char **argv) {
  int port = atoi(argv[1]);

  BenchmarkServer server(port);
  server.Run();
}