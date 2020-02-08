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
using ray::rpc::StreamEchoRequest;
using ray::rpc::ClientCallManager;

class BenchmarkClient {
public:
  explicit BenchmarkClient(const std::string &address, const int port)
    : call_manager_(io_service_),
      test_client_(address, port, call_manager_) {}

  void Run(const std::string &str) {

    boost::asio::io_service::work work(io_service_);
    std::thread thread = std::thread(&BenchmarkClient::RunIOService, this);

    uint64_t request_id = 0;
    while (true) {
      // cout << "send request " << request_id << endl;
      auto request = std::make_shared<StreamEchoRequest>();
      request->set_request_id(++request_id);
      request->set_request_message(str);
      test_client_.StreamEcho(request, /* TODO:callback */ nullptr);
    }
  }

private:
  
  void RunIOService() { io_service_.run(); }

  boost::asio::io_service io_service_;
  ClientCallManager call_manager_;
  EchoClient test_client_;


};

int main(int argc, char **argv) {
  std::string ip = std::string(argv[1]);
  int port = std::stoi(argv[2]);
  size_t payload_size = std::stoi(argv[3]);

  std::string a;
  a.assign(payload_size, 'a');

  BenchmarkClient client(ip, port);

  client.Run(a);

  return 0;
}