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

using ray::rpc::StreamWriter;

volatile uint64_t sent = 0;
volatile uint64_t received = 0;

class BenchmarkClient {
public:
  explicit BenchmarkClient()
    : writer_([this] (const StreamEchoRequest &request){
        // stream_->Write(request, reinterpret_cast<void *>(tag_)); 
        counter_ = request.request_id();

        for (int i = 0; i < 10; i++) {
          auto r = std::make_shared<StreamEchoRequest>();
        }

      }),
      start_(std::chrono::system_clock::now()),
      end_(std::chrono::system_clock::now()),
      counter_(1)  {}

  void Run(const std::string &str) {

    boost::asio::io_service::work work(io_service_);
    std::thread io_thread = std::thread(&BenchmarkClient::RunIOService, this);


    std::thread writer_thread = std::thread([this, str] {

      uint64_t request_id = 0;
      while (true) {
        sent = request_id;

        // cout << "send request " << request_id << endl;
        auto request = std::make_shared<StreamEchoRequest>();
        request->set_request_id(++request_id);
        request->set_request_message(str);

/*
        counter_ = request->request_id();

        for (int i = 0; i < 10; i++) {
          auto r = std::make_shared<StreamEchoRequest>();
        }
*/

        // writer_.SetReadyToWrite(true);
        writer_.WriteStream(request);
/*
        auto batch_count = 10000;
        if (counter_ % batch_count == 0) {
          end_ = std::chrono::system_clock::now();
          std::chrono::duration<double> diff = end_ - start_;
          // double gbps = 8.0 * onehm / diff.count() / 1e9;
          double gbps = batch_count / diff.count() / 1000;
          std::cout << gbps << " K, sent: " << sent
                    << ", received: " << counter_ << std::endl;
          start_ = end_;     
        }
      */
      }
    });

    std::thread reader_thread = std::thread([this, str] {

      while (true) {
        writer_.OnStreamWritten();

        auto batch_count = 10000;
        if (counter_ % batch_count == 0) {
          end_ = std::chrono::system_clock::now();
          std::chrono::duration<double> diff = end_ - start_;
          // double gbps = 8.0 * onehm / diff.count() / 1e9;
          double gbps = batch_count / diff.count() / 1000;
          std::cout << gbps << " K, sent: " << sent
                    << ", received: " << counter_ << std::endl;
          start_ = end_;     
        }

      }
    });

    reader_thread.join();

    writer_thread.join();





#if 0
    test_client_.StartStreamEcho();

    uint64_t request_id = 0;
    while (true) {
      sent = request_id;

      while (sent > received + 5000) {

      }

      // cout << "send request " << request_id << endl;
      auto request = std::make_shared<StreamEchoRequest>();
      request->set_request_id(++request_id);
      request->set_request_message(str);
      test_client_.StreamEcho(request, [this] (
          const ray::Status &status, const StreamEchoReply &reply) {

        received = reply.request_id();

        auto batch_count = 10000;
        if (reply.request_id() % batch_count == 0) {
          end_ = std::chrono::system_clock::now();
          std::chrono::duration<double> diff = end_ - start_;
          // double gbps = 8.0 * onehm / diff.count() / 1e9;
          double gbps = batch_count / diff.count() / 1000;
          std::cout << gbps << " K, sent: " << sent
                    << ", received: " << received << std::endl;
          start_ = end_;     
        }

         //std::cout << "received reply " << reply.request_id() << std::endl;   
      });
    }
#endif
  }

private:
  
  void RunIOService() { io_service_.run(); }

  boost::asio::io_service io_service_;

  StreamWriter<StreamEchoRequest> writer_;

  std::chrono::system_clock::time_point start_;
  std::chrono::system_clock::time_point end_;

  std::atomic<uint64_t> counter_;
};

int main(int argc, char **argv) {
  size_t payload_size = std::stoi(argv[1]);

  std::string a;
  a.assign(payload_size, 'a');

  BenchmarkClient client;
  client.Run(a);

  return 0;
}