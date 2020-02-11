#include <chrono>
#include <fstream>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/support/channel_arguments.h>
#include <string>
#include <thread>
#include <unistd.h>
#include <boost/asio.hpp>

#include "src/ray/protobuf/test.grpc.pb.h"
#include "src/ray/rpc/test/echo_client.h"

using namespace std;

using grpc::Channel;
using grpc::ClientAsyncReaderWriter;
using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::Status;

using ray::rpc::EchoClient;
using ray::rpc::EchoRequest;
using ray::rpc::EchoReply;
using ray::rpc::StreamEchoRequest;
using ray::rpc::StreamEchoReply;
using ray::rpc::ClientCallManager;

volatile uint64_t sent = 0;
volatile uint64_t received = 0;

class BenchmarkClient {
public:
  explicit BenchmarkClient(const std::string &address, const int port, bool is_stream)
    : call_manager_(io_service_),
      test_client_(address, port, call_manager_),
      start_(std::chrono::system_clock::now()),
      end_(std::chrono::system_clock::now()),
      is_stream_(is_stream)  {}

  void Run(const std::string &str) {

    boost::asio::io_service::work work(io_service_);
    std::thread thread = std::thread(&BenchmarkClient::RunIOService, this);

    if (is_stream_) {
      test_client_.StartStreamEcho();
    }

    uint64_t request_id = 0;
    while (true) {
      sent = request_id;
   
      while (sent > received + 5000) {

      }

      // cout << "send request " << request_id << endl;
      if (is_stream_) {
        auto request = std::make_shared<StreamEchoRequest>();
        request->set_request_id(++request_id);
        request->set_request_message(str);
        test_client_.StreamEcho(request, [this] (
            const ray::Status &status, const StreamEchoReply &reply) {

          received = reply.request_id();
          MayReportPerf(sent, received);
        });
      } else {
        auto request = std::make_shared<EchoRequest>();
        request->set_request_id(++request_id);
        request->set_request_message(str);
        test_client_.Echo(*request, [this] (
            const ray::Status &status, const EchoReply &reply) {

        // cout << "received unary reply, status: " << status.ok() << endl;
          received = reply.request_id();
          MayReportPerf(sent, received);
        });
      }
    }
  }

private:
  
  void MayReportPerf(uint64_t sent, uint64_t received) {
    auto batch_count = 10000;
    if (received % batch_count == 0) {
      end_ = std::chrono::system_clock::now();
      std::chrono::duration<double> diff = end_ - start_;
      // double gbps = 8.0 * onehm / diff.count() / 1e9;
      double gbps = batch_count / diff.count() / 1000;
      std::cout << gbps << " K, sent: " << sent
                << ", received: " << received << std::endl;
      start_ = end_;     
    }
  }

  void RunIOService() { io_service_.run(); }

  boost::asio::io_service io_service_;
  ClientCallManager call_manager_;
  EchoClient test_client_;

  std::chrono::system_clock::time_point start_;
  std::chrono::system_clock::time_point end_;

  bool is_stream_ = true;
};

int main(int argc, char **argv) {
  std::string ip = std::string(argv[1]);
  int port = std::stoi(argv[2]);
  size_t payload_size = std::stoi(argv[3]);
  std::string mode = std::string(argv[4]);
  bool is_stream = true;
  if (mode == "unary") {
    is_stream = false;
  }


  std::string a;
  a.assign(payload_size, 'a');

  BenchmarkClient client(ip, port, is_stream);

  client.Run(a);

  return 0;
}