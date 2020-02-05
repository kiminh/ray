#include "src/ray/rpc/grpc_server.h"

#include <grpcpp/impl/service_type.h>
#include <boost/asio/detail/socket_holder.hpp>

namespace ray {
namespace rpc {

GrpcServer::GrpcServer(std::string name, const uint32_t port, int num_threads)
    : name_(std::move(name)), port_(port), is_closed_(true), num_threads_(num_threads) {
  cqs_.reserve(num_threads_);
}

void GrpcServer::Run() {
  uint32_t specified_port = port_;
  std::string server_address("0.0.0.0:" + std::to_string(port_));

  grpc::ServerBuilder builder;
  // Disable the SO_REUSEPORT option. We don't need it in ray. If the option is enabled
  // (default behavior in grpc), we may see multiple workers listen on the same port and
  // the requests sent to this port may be handled by any of the workers.
  builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  if (services_.empty()) {
    RAY_LOG(WARNING) << "No service is found when start grpc server " << name_;
  }
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  for (int i = 0; i < num_threads_; i++) {
    cqs_.push_back(builder.AddCompletionQueue());
  }
  // Build and start server.
  server_ = builder.BuildAndStart();
  // If the grpc server failed to bind the port, the `port_` will be set to 0.
  RAY_CHECK(port_ > 0)
      << "Port " << specified_port
      << " specified by caller already in use. Try passing node_manager_port=... into "
         "ray.init() to pick a specific port";
  RAY_LOG(INFO) << name_ << " server started, listening on port " << port_ << ".";

  // Create calls for all the server call factories.
  for (auto &entry : server_call_factories_and_concurrencies_) {
    for (int i = 0; i < entry.second; i++) {
      // Create and request calls from the factory.
      entry.first->CreateCall();
    }
  }
  // Start threads that polls incoming requests.
  for (int i = 0; i < num_threads_; i++) {
    polling_threads_.emplace_back(&GrpcServer::PollEventsFromCompletionQueue, this, i);
  }
  // Set the server as running.
  is_closed_ = false;
}

void GrpcServer::RegisterService(GrpcService &service) {
  services_.emplace_back(service.GetGrpcService());

  if (service.HasStreamCall()) {
    // If the RPC service has stream calls, then this service will only use the
    // first cq because we need to gurantee processing order for stream call.
    service.InitServerCallFactories(cqs_[0], &server_call_factories_and_concurrencies_);
  } else {
    // For other RPC services, start with cq #1 if there are multiple cqs configured.
    for (int i = (num_threads_ > 1 ? 1 : 0); i < num_threads_; i++) {
      service.InitServerCallFactories(cqs_[i], &server_call_factories_and_concurrencies_);
    }
  }
}


void GrpcServer::ProcessUnaryCall(
    ServerUnaryCall *server_call, bool ok) {
    bool delete_call = false;
  if (ok) {
    switch (server_call->GetState()) {
    case ServerUnaryCall::CallState::PENDING:
      // We've received a new incoming request. Now this call object is used to
      // track this request.
      server_call->SetState(ServerUnaryCall::CallState::PROCESSING);
      server_call->HandleRequest();
      break;
    case ServerUnaryCall::CallState::SENDING_REPLY:
      // GRPC has sent reply successfully, invoking the callback.
      server_call->OnReplySent();
      // The rpc call has finished and can be deleted now.
      delete_call = true;
      break;
    default:
      RAY_LOG(FATAL) << "Shouldn't reach here.";
      break;
    }
  } else {
    // `ok == false` will occur in two situations:
    // First, the server has been shut down, the server call's status is PENDING
    // Second, server has sent reply to client and failed, the server call's status is
    // SENDING_REPLY
    if (server_call->GetState() == ServerUnaryCall::CallState::SENDING_REPLY) {
      server_call->OnReplyFailed();
    }
    delete_call = true;
  }
  if (delete_call) {
    delete server_call;
  }
}

void GrpcServer::ProcessStreamCall(ServerStreamCall *server_call,
                                   bool is_reply, bool ok) {
  if (ok) {
    if (is_reply) {
      server_call->OnReplyWritten();
    } else {
      switch (server_call->GetState()) {
      case ServerStreamCall::CallState::CONNECTING:
        server_call->OnConnectFinished();
        break;
      case ServerStreamCall::CallState::READING:
        server_call->HandleRequest();
        break;
/*
      case ServerStreamCall::CallState::FINISHED:
        server_call->DeleteTag();
        RAY_LOG(INFO)
            << "Stream server received received `FINISH` from completion queue.";
        break;
*/
      default:
        RAY_LOG(FATAL) << "Incorrect server call state "
                       << static_cast<int>(server_call->GetState());
        break;
      }
    }
  } else {
/*
    if (is_reply) {
      server_call->DeleteReplyTag();
    } else {
      server_call->Finish();
    }
*/
  }
}

void GrpcServer::PollEventsFromCompletionQueue(int index) {
  void *got_tag;
  bool ok;
  // Keep reading events from the `CompletionQueue` until it's shutdown.
  while (cqs_[index]->Next(&got_tag, &ok)) {
    auto tag = reinterpret_cast<ServerCallTag *>(got_tag);
    bool is_reply = tag->IsReply();
    auto call = tag->GetCall();
    auto call_type = call->GetType();
    switch (call_type) {
    case ServerCallType::UNARY:
      ProcessUnaryCall(reinterpret_cast<ServerUnaryCall *>(call.get()), ok);
      break;
    case ServerCallType::STREAM:
      ProcessStreamCall(reinterpret_cast<ServerStreamCall *>(call.get()), is_reply, ok);
      break;
    default:
      RAY_LOG(FATAL) << "Shouldn't reach here, unrecognized type: "
                     << static_cast<int>(call_type);
      break;
    }
  }
}

}  // namespace rpc
}  // namespace ray
