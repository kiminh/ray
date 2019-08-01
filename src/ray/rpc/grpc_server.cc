
#include "src/ray/rpc/grpc_server.h"
#include <grpcpp/impl/service_type.h>

namespace ray {
namespace rpc {

void GrpcServer::Run() {
  std::string server_address;
  // Set unix domain socket or tcp address.
  if (!unix_socket_path_.empty()) {
    server_address = "unix://" + unix_socket_path_;
  } else {
    server_address = "0.0.0.0:" + std::to_string(port_);
  }

  grpc::ServerBuilder builder;
  // TODO(hchen): Add options for authentication.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials(), &port_);
  // Register all the services to this server.
  if (services_.empty()) {
    RAY_LOG(WARNING) << "No service found when start grpc server " << name_;
  }
  for (auto &entry : services_) {
    builder.RegisterService(&entry.get());
  }
  // Get hold of the completion queue used for the asynchronous communication
  // with the gRPC runtime.
  cq_ = builder.AddCompletionQueue();
  // Build and start server.
  server_ = builder.BuildAndStart();
  if (unix_socket_path_.empty()) {
    // For a TCP-based server, the actual port is decided after `AddListeningPort`.
    server_address = "0.0.0.0:" + std::to_string(port_);
  }
  RAY_LOG(INFO) << name_ << " server started, listening on " << server_address;

  // Create calls for all the server call factories.
  for (auto &entry : server_call_factories_and_concurrencies_) {
    for (int i = 0; i < entry.second; i++) {
      // Create and request calls from the factory.
      entry.first->CreateCall();
    }
  }
  // Create stream calls for all the stream call factories.
  for (auto &entry : server_stream_call_factories_) {
    entry->CreateCall();
  }
  // Start a thread that polls incoming requests.
  polling_thread_ = std::thread(&GrpcServer::PollEventsFromCompletionQueue, this);
  // Set the server as running.
  is_closed_ = false;
}

void GrpcServer::RegisterService(GrpcService &service) {
  services_.emplace_back(service.GetGrpcService());
  service.InitServerCallFactories(cq_, &server_call_factories_and_concurrencies_,
                                  &server_stream_call_factories_);
}

void GrpcServer::PollEventsFromCompletionQueue() {
  void *got_tag;
  bool ok;
  // Keep reading events from the `CompletionQueue` until it's shutdown.
  while (cq_->Next(&got_tag, &ok)) {
    auto *tag = static_cast<ServerCallTag *>(got_tag);
    auto server_call = tag->GetCall();
    bool delete_call = false;
    if (ok) {
      if (tag->IsDoneTag()) {
        RAY_LOG(INFO) << "Server receive a done tag.";
        delete server_call->GetReplyWriterTag();
        delete server_call->GetDoneTag();
        RAY_LOG(INFO) << "After finish.";
      } else if (tag->IsWriterTag()) {
        server_call->AsyncWriteNextReply();
        RAY_LOG(INFO) << "Server receive a writer tag.";
      } else {
        switch (server_call->GetState()) {
        case ServerCallState::CONNECT:
          server_call->AsyncReadNextRequest();
          break;
        case ServerCallState::PENDING:
          // We've received a new incoming request. Now this call object is used to
          // track this request. So we need to create another call to handle next
          // incoming request.
          if (server_call->GetCallType() == ServerCallType::DEFAULT_ASYNC_CALL) {
            server_call->GetFactory().CreateCall();
          }
          server_call->SetState(ServerCallState::PROCESSING);
          server_call->HandleRequest();
          break;
        case ServerCallState::PROCESSING:
          break;
        case ServerCallState::SENDING_REPLY:
          // GRPC has sent reply successfully, invoking the callback.
          server_call->OnReplySent();
          // The rpc call has finished and can be deleted now.
          delete_call = true;
          break;
        case ServerCallState::FINISH:
          RAY_LOG(INFO) << "Received FINISH state.";
          delete server_call->GetReplyWriterTag();
          delete server_call->GetDoneTag();
          delete tag;
        default:
          RAY_LOG(FATAL) << "Shouldn't reach here.";
          break;
        }
      }
    } else {
      RAY_LOG(INFO) << "ok == false, tag type: " << static_cast<int>(tag->GetType())
                    << ", call type: " << static_cast<int>(tag->GetCall()->GetCallType());
      if (tag->GetCall()->GetCallType() == ServerCallType::STREAM_ASYNC_CALL) {
      } else {
        // `ok == false` will occur in two situations:
        // First, the server has been shut down, the server call's status is PENDING
        // Second, server has sent reply to client and failed, the server call's status is
        // SENDING_REPLY
        if (server_call->GetState() == ServerCallState::SENDING_REPLY) {
          server_call->OnReplyFailed();
        }
      }
      delete_call = true;
    }
    if (delete_call) {
      delete tag;
    }
  }
}

}  // namespace rpc
}  // namespace ray
