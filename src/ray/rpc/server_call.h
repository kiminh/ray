#ifndef RAY_RPC_SERVER_CALL_H
#define RAY_RPC_SERVER_CALL_H

#include <queue>
#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>
#include "absl/synchronization/mutex.h"

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"

namespace ray {
namespace rpc {

enum class ServerCallType {
  UNARY,
  STREAM,
};

/// Represents the callback function to be called when a `ServiceHandler` finishes
/// handling a request.
/// \param status The status would be returned to client.
/// \param success Success callback which will be invoked when the reply is successfully
/// sent to the client.
/// \param failure Failure callback which will be invoked when the reply fails to be
/// sent to the client.
using SendReplyCallback = std::function<void(Status status, std::function<void()> success,
                                             std::function<void()> failure)>;

class ServerCallFactory;

/// Represents an incoming request of a gRPC server.
///
/// The lifecycle and state transition of a `ServerCall` is as follows:
///
/// --(1)--> PENDING --(2)--> PROCESSING --(3)--> SENDING_REPLY --(4)--> [FINISHED]
///
/// (1) The `GrpcServer` creates a `ServerCall` and use it as the tag to accept requests
///     gRPC `CompletionQueue`. Now the state is `PENDING`.
/// (2) When a request is received, an event will be gotten from the `CompletionQueue`.
///     `GrpcServer` then should change `ServerCall`'s state to PROCESSING and call
///     `ServerCall::HandleRequest`.
/// (3) When the `ServiceHandler` finishes handling the request, `ServerCallImpl::Finish`
///     will be called, and the state becomes `SENDING_REPLY`.
/// (4) When the reply is sent, an event will be gotten from the `CompletionQueue`.
///     `GrpcServer` will then delete this call.
///
/// NOTE(hchen): Compared to `ServerCallImpl`, this abstract interface doesn't use
/// template. This allows the users (e.g., `GrpcServer`) not having to use
/// template as well.
class ServerCall {
 public:
  virtual ServerCallType GetType() const = 0;

  /// Virtual destruct function to make sure subclass would destruct properly.
  virtual ~ServerCall() = default;
};

/// This class wraps a `ServerCall`, and is used as the `tag` of gRPC's `CompletionQueue`.
class ServerCallTag {
 public:
  /// Constructor.
  ///
  /// \param call A `ServerCall` that represents a request or a stream.
  explicit ServerCallTag(std::shared_ptr<ServerCall> call,
      bool is_reply = false) : call_(std::move(call)), is_reply_(is_reply) {}

  /// Get the wrapped `ServerCall`.
  const std::shared_ptr<ServerCall> &GetCall() const { return call_; }

  /// Whether this tag corresponds to a reply.
  bool IsReply() const { return is_reply_; }

 private:
  std::shared_ptr<ServerCall> call_;
  /// Whether this tag refers to a request or reply.
  bool is_reply_;
};

/// The factory that creates a particular kind of `ServerCall` objects.
class ServerCallFactory {
 public:
  /// Create a new `ServerCall` and request gRPC runtime to start accepting the
  /// corresponding type of requests.
  ///
  /// \return Pointer to the `ServerCall` object.
  virtual void CreateCall() const = 0;

  virtual ~ServerCallFactory() = default;
};


/// Represents the generic signature of a `FooServiceHandler::HandleBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
using HandleRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                       SendReplyCallback);


class ServerUnaryCall : public ServerCall {
 public:
  /// Represents state of a `ServerCall`.
  enum class CallState {
    /// The call is created and waiting for an incoming request.
    PENDING,
    /// Request is received and being processed.
    PROCESSING,
    /// Request processing is done, and reply is being sent to client.
    SENDING_REPLY
  };

  ServerCallType GetType() const override { return ServerCallType::UNARY; }

  /// Handle the requst. This is the callback function to be called by
  /// `GrpcServer` when the request is received.
  virtual void HandleRequest() = 0;

  /// Invoked when sending reply successes.
  virtual void OnReplySent() = 0;

  // Invoked when sending reply fails.
  virtual void OnReplyFailed() = 0;

  virtual CallState GetState() const = 0;

  virtual void SetState(const CallState &new_state) = 0;
};

/// Implementation of `ServerCall`. It represents `ServerCall` for a particular
/// RPC method.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
class ServerUnaryCallImpl : public ServerUnaryCall {
 public:
  /// Constructor.
  ///
  /// \param[in] factory The factory which created this call.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] io_service The event loop.
  ServerUnaryCallImpl(
      const ServerCallFactory &factory, ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      boost::asio::io_service &io_service)
      : state_(CallState::PENDING),
        factory_(factory),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        response_writer_(&context_),
        io_service_(io_service) {}

  CallState GetState() const override { return state_; }

  void SetState(const CallState &new_state) override { state_ = new_state; }

  void HandleRequest() override {
    if (!io_service_.stopped()) {
      io_service_.post([this] { HandleRequestImpl(); });
    } else {
      // Handle service for rpc call has stopped, we must handle the call here
      // to send reply and remove it from cq
      RAY_LOG(DEBUG) << "Handle service has been closed.";
      SendReply(Status::Invalid("HandleServiceClosed"));
    }
  }

  void HandleRequestImpl() {
    state_ = CallState::PROCESSING;
    // NOTE(hchen): This `factory` local variable is needed. Because `SendReply` runs in
    // a different thread, and will cause `this` to be deleted.
    const auto &factory = factory_;
    (service_handler_.*handle_request_function_)(
        request_, &reply_,
        [this](Status status, std::function<void()> success,
               std::function<void()> failure) {
          // These two callbacks must be set before `SendReply`, because `SendReply`
          // is async and this `ServerCall` might be deleted right after `SendReply`.
          send_reply_success_callback_ = std::move(success);
          send_reply_failure_callback_ = std::move(failure);

          // When the handler is done with the request, tell gRPC to finish this request.
          // Must send reply at the bottom of this callback, once we invoke this funciton,
          // this server call might be deleted
          SendReply(status);
        });
    // We've finished handling this request,
    // create a new `ServerCall` to accept the next incoming request.
    factory.CreateCall();
  }

  void OnReplySent() override {
    if (send_reply_success_callback_ && !io_service_.stopped()) {
      auto callback = std::move(send_reply_success_callback_);
      io_service_.post([callback]() { callback(); });
    }
  }

  void OnReplyFailed() override {
    if (send_reply_failure_callback_ && !io_service_.stopped()) {
      auto callback = std::move(send_reply_failure_callback_);
      io_service_.post([callback]() { callback(); });
    }
  }

 private:
  /// Tell gRPC to finish this request and send reply asynchronously.
  void SendReply(const Status &status) {
    state_ = CallState::SENDING_REPLY;
    response_writer_.Finish(reply_, RayStatusToGrpcStatus(status), this);
  }

  CallState state_;

  /// The factory which created this call.
  const ServerCallFactory &factory_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// Context for the request, allowing to tweak aspects of it such as the use
  /// of compression, authentication, as well as to send metadata back to the client.
  grpc::ServerContext context_;

  /// The response writer.
  grpc_impl::ServerAsyncResponseWriter<Reply> response_writer_;

  /// The event loop.
  boost::asio::io_service &io_service_;

  /// The request message.
  Request request_;

  /// The reply message.
  Reply reply_;

  /// The callback when sending reply successes.
  std::function<void()> send_reply_success_callback_ = nullptr;

  /// The callback when sending reply fails.
  std::function<void()> send_reply_failure_callback_ = nullptr;

  template <class T1, class T2, class T3, class T4>
  friend class ServerCallFactoryImpl;
};

/// Represents the generic signature of a `FooService::AsyncService::RequestBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using RequestUnaryCallFunction = void (GrpcService::AsyncService::*)(
    grpc::ServerContext *, Request *, grpc_impl::ServerAsyncResponseWriter<Reply> *,
    grpc::CompletionQueue *, grpc::ServerCompletionQueue *, void *);

/// Implementation of `ServerCallFactory`
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class ServiceHandler, class Request, class Reply>
class ServerCallFactoryImpl : public ServerCallFactory {
  using AsyncService = typename GrpcService::AsyncService;

 public:
  /// Constructor.
  ///
  /// \param[in] service The gRPC-generated `AsyncService`.
  /// \param[in] request_call_function Pointer to the `AsyncService::RequestMethod`
  //  function.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] cq The `CompletionQueue`.
  /// \param[in] io_service The event loop.
  ServerCallFactoryImpl(
      AsyncService &service,
      RequestUnaryCallFunction<GrpcService, Request, Reply> request_call_function,
      ServiceHandler &service_handler,
      HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      boost::asio::io_service &io_service)
      : service_(service),
        request_call_function_(request_call_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        cq_(cq),
        io_service_(io_service) {}

  void CreateCall() const override {
    // Create a new `ServerCall`. This object will eventually be deleted by
    // `GrpcServer::PollEventsFromCompletionQueue`.
    auto call = new ServerUnaryCallImpl<ServiceHandler, Request, Reply>(
        *this, service_handler_, handle_request_function_, io_service_);
    /// Request gRPC runtime to starting accepting this kind of request, using the call as
    /// the tag.
    (service_.*request_call_function_)(&call->context_, &call->request_,
                                       &call->response_writer_, cq_.get(), cq_.get(),
                                       call);
  }

 private:
  /// The gRPC-generated `AsyncService`.
  AsyncService &service_;

  /// Pointer to the `AsyncService::RequestMethod` function.
  RequestUnaryCallFunction<GrpcService, Request, Reply> request_call_function_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// The `CompletionQueue`.
  const std::unique_ptr<grpc::ServerCompletionQueue> &cq_;

  /// The event loop.
  boost::asio::io_service &io_service_;
};

///
/// GRPC stream implementation.
///
class ServerStreamCall : public ServerCall {
 public:
  /// Represents state of a `ServerCall`.
  enum class CallState {
    /// The call is created and waiting for an incoming request.
    CONNECTING,
    /// Request is received and being processed.
    READING,
    /// This stream has finished.
    FINISHED,
  };

  ServerCallType GetType() const override { return ServerCallType::STREAM; }

  virtual CallState GetState() const = 0;

  virtual void SetState(const CallState &new_state) = 0;

  virtual void OnConnectFinished() = 0;

  virtual void HandleRequest() = 0; 

  virtual void OnReplyWritten() = 0;
};


using SendStreamReplyCallback = std::function<void()>;

/// Represents the generic signature of a `FooServiceHandler::HandleBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
using HandleStreamRequestFunction = void (ServiceHandler::*)(const Request &, Reply *,
                                                       SendStreamReplyCallback);

/// Implementation of `ServerCall`. It represents `ServerCall` for a particular
/// RPC method.
///
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class ServiceHandler, class Request, class Reply>
class ServerStreamCallImpl : public ServerStreamCall {
 public:
  /// Constructor.
  ///
  /// \param[in] factory The factory which created this call.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] io_service The event loop.
  ServerStreamCallImpl(
      const ServerCallFactory &factory, ServiceHandler &service_handler,
      HandleStreamRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      boost::asio::io_service &io_service)
      : state_(CallState::CONNECTING),
        factory_(factory),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        stream_(&context_),
        io_service_(io_service) {}

  CallState GetState() const override { return state_; }

  void SetState(const CallState &new_state) override { state_ = new_state; }

  void OnConnectFinished() override {
    // We shouldn't setup these tags unless a stream call has connected to the server, or
    // we would never get the tags from the completion queue.
    auto reply_tag = new ServerCallTag(tag_->GetCall(), true);
    SetReplyTag(reply_tag);

    // It's ready to read from cq.
    ReadNextRequest();
  }

  void HandleRequest() override {
    // NOTE(zhijunfu): The actual handler is run in another thread,
    // use a temporay variable to hold the request to be processed,
    // so that we can require the next read tag from cq, instead of
    // waiting for the request to be processed.
    std::shared_ptr<Request> request = std::make_shared<Request>();
    std::swap(request, request_);

    HandleRequest(request);

    ReadNextRequest();
  }

  void OnReplyWritten() override {
    WriteNextReply();
  }

 private:
  /// Only one write may be outstanding at any given time. This means that
  /// after calling `Write`, one must wait to receive a tag from the completion
  /// queue before calling `Write` again. We should put reply into the buffer to avoid
  /// calling `Write` before getting previous tag from completion queue.
  void Write(std::shared_ptr<Reply> reply) {
    write_mutex_.Lock();
    if (ready_to_write_) {
      ready_to_write_ = false;
      write_mutex_.Unlock();
      stream_.Write(*reply, reinterpret_cast<void *>(reply_tag_));
    } else {
      pending_replies_.emplace(std::move(reply));
      write_mutex_.Unlock();
    }
  }

  void WriteNextReply() {
    write_mutex_.Lock();
    if (pending_replies_.empty()) {
      ready_to_write_ = true;
      write_mutex_.Unlock();
    } else {
      ready_to_write_ = false;
      auto request = pending_replies_.front();
      pending_replies_.pop();
      write_mutex_.Unlock();
      stream_.Write(*request, reinterpret_cast<void *>(reply_tag_));
    }
  }

  void SetRequestTag(ServerCallTag *tag) { tag_ = tag; }

  void SetReplyTag(ServerCallTag *reply_tag) { reply_tag_ = reply_tag; }

  // ServerCallTag *GetReplyTag() override { return reply_tag_; }

  void HandleRequest(std::shared_ptr<Request> request) {
    if (!io_service_.stopped()) {
      io_service_.post([this, request] { HandleRequestImpl(request); });
    } else {
      // Handle service for rpc call has stopped, we must handle the call here
      // to send reply and remove it from cq
      RAY_LOG(DEBUG) << "Handle service has been closed.";
      
      // TODO: how to handle this?
      //SendReply(Status::Invalid("HandleServiceClosed"));
    }
  }

  void HandleRequestImpl(std::shared_ptr<Request> request) {
    auto reply = std::make_shared<Reply>();
    (service_handler_.*handle_request_function_)(
        *request, reply.get(),
        [this, reply]() {
          // Send reply to client.
          Write(reply);
        });
  }

  void ReadNextRequest() {
    stream_.Read(request_.get(), reinterpret_cast<void *>(tag_));
  }

 private:

  /// State of this call.
  CallState state_;

  /// The factory which created this call.
  const ServerCallFactory &factory_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Pointer to the service handler function.
  HandleStreamRequestFunction<ServiceHandler, Request, Reply> handle_request_function_;

  /// Context for the request, allowing to tweak aspects of it such as the use
  /// of compression, authentication, as well as to send metadata back to the client.
  grpc::ServerContext context_;

  /// Async reader writer.
  grpc_impl::ServerAsyncReaderWriter<Reply, Request> stream_;

  /// The event loop.
  boost::asio::io_service &io_service_;

  /// The request message.
  std::shared_ptr<Request> request_;

  /// The reply message.
  std::shared_ptr<Reply> reply_;

  /// Server call tag used to read request.
  ServerCallTag *tag_;

  /// Server call tag used to write reply.
  ServerCallTag *reply_tag_;

  /// Mutex to protect ready_to_write_ and pending_requests_ fields.
  absl::Mutex write_mutex_;

  /// Whether it's ready to write to this stream.
  bool ready_to_write_ GUARDED_BY(write_mutex_);

  // Buffer for sending replies to the client. We need write the request into the queue
  // when it is not ready to write.
  std::queue<std::shared_ptr<Reply>> pending_replies_ GUARDED_BY(write_mutex_);

  template <class T1, class T2, class T3, class T4>
  friend class ServerStreamCallFactoryImpl;
};


/// Represents the generic signature of a `FooService::AsyncService::RequestBar()`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using RequestStreamCallFunction = void (GrpcService::AsyncService::*)(
    grpc::ServerContext *, grpc_impl::ServerAsyncReaderWriter<Reply, Request> *,
    grpc::CompletionQueue *, grpc::ServerCompletionQueue *, void *);

/// Stream server call factory, it's a kind of implementation of `ServerCallFactory`.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam ServiceHandler Type of the handler that handles the request.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class ServiceHandler, class Request, class Reply>
class ServerStreamCallFactoryImpl : public ServerCallFactory {
  using AsyncService = typename GrpcService::AsyncService;

 public:
  /// Constructor.
  ///
  /// \param[in] service The gRPC-generated `AsyncService`.
  /// \param[in] request_call_function Pointer to the `AsyncService::RequestMethod`
  //  function.
  /// \param[in] service_handler The service handler that handles the request.
  /// \param[in] handle_request_function Pointer to the service handler function.
  /// \param[in] cq The `CompletionQueue`.
  /// \param[in] io_service The event loop.
  ServerStreamCallFactoryImpl(
      AsyncService &service,
      RequestStreamCallFunction<GrpcService, Request, Reply> request_call_function,
      ServiceHandler &service_handler,
      HandleStreamRequestFunction<ServiceHandler, Request, Reply> handle_request_function,
      const std::unique_ptr<grpc::ServerCompletionQueue> &cq,
      boost::asio::io_service &io_service)
      : service_(service),
        request_call_function_(request_call_function),
        service_handler_(service_handler),
        handle_request_function_(handle_request_function),
        cq_(cq),
        io_service_(io_service) {}

  void CreateCall() const override {
    auto call = std::make_shared<ServerStreamCallImpl<ServiceHandler, Request, Reply>>(
        *this, service_handler_, handle_request_function_, io_service_);
    auto tag = new ServerCallTag(call, false);
    call->SetRequestTag(tag);
    call->SetState(ServerStreamCall::CallState::CONNECTING);
    (service_.*request_call_function_)(
        &call->context_, &(call->stream_), cq_.get(), cq_.get(),
        reinterpret_cast<void *>(tag));
  }

 private:
  /// The gRPC-generated `AsyncService`.
  AsyncService &service_;

  /// The `AsyncService::RequestMethod` function.
  RequestStreamCallFunction<GrpcService, Request, Reply> request_call_function_;

  /// The service handler that handles the request.
  ServiceHandler &service_handler_;

  /// Service handler function to handle this server call.
  HandleStreamRequestFunction<ServiceHandler, Request, Reply>
      handle_request_function_;

  /// The `CompletionQueue`.
  const std::unique_ptr<grpc::ServerCompletionQueue> &cq_;

  /// The event loop.
  boost::asio::io_service &io_service_;
};

}  // namespace rpc
}  // namespace ray

#endif
