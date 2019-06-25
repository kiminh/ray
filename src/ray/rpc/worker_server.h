#ifndef RAY_RPC_WORKER_SERVER_H
#define RAY_RPC_WORKER_SERVER_H

#include "ray/rpc/grpc_server.h"
#include "ray/rpc/server_call.h"

#include "src/ray/protobuf/worker.grpc.pb.h"
#include "src/ray/protobuf/worker.pb.h"

namespace ray {
namespace rpc {

/// Interface of the `WorkerService`, see `src/ray/protobuf/worker.proto`.
class WorkerTaskHandler {
 public:
  /// Handle a `PushTask` request.
  /// The implementation can handle this request asynchronously. When hanling is done, the
  /// `done_callback` should be called.
  ///
  /// \param[in] request The request message.
  /// \param[out] reply The reply message.
  /// \param[in] done_callback The callback to be called when the request is done.
  virtual void HandlePushTask(const PushTaskRequest &request,
                                 PushTaskReply *reply,
                                 RequestDoneCallback done_callback) = 0;
};

/// The `GrpcServer` for `WorkerService`.
class WorkerTaskGrpcService : public GrpcService {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service See super class.
  /// \param[in] handler The service handler that actually handle the requests.
  WorkerTaskGrpcService(boost::asio::io_service &main_service,
               WorkerTaskHandler &service_handler)
      : GrpcService(main_service),
        service_handler_(service_handler){};

 protected:
  grpc::Service &GetGrpcService() override { return service_; }

  void InitServerCallFactories(
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      std::vector<std::pair<std::unique_ptr<ServerCallFactory>, int>>
          *server_call_factories_and_concurrencies) override {
    // Initialize the Factory for `PushTask` requests.
    std::unique_ptr<ServerCallFactory> push_task_call_Factory(
        new ServerCallFactoryImpl<WorkerTaskService, WorkerTaskHandler,
                                  PushTaskRequest, PushTaskReply>(
            service_, &WorkerTaskService::AsyncService::RequestPushTask,
            service_handler_, &WorkerTaskHandler::HandlePushTask, cq,
            main_service_));

    // Set `PushTask`'s accept concurrency to 100.
    server_call_factories_and_concurrencies->emplace_back(
        std::move(push_task_call_Factory), 100);
  }

 private:
  /// The grpc async service object.
  WorkerTaskService::AsyncService service_;

  /// The service handler that actually handle the requests.
  WorkerTaskHandler &service_handler_;
};

}  // namespace rpc
}  // namespace ray

#endif
