#ifndef RAY_RPC_CLIENT_CALL_H
#define RAY_RPC_CLIENT_CALL_H

#include <queue>
#include <grpcpp/grpcpp.h>
#include <boost/asio.hpp>
#include "absl/synchronization/mutex.h"

#include "ray/common/grpc_util.h"
#include "ray/common/status.h"
#include "ray/rpc/stream_writer.h"

#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>

namespace ray {
namespace rpc {

/// Represents an outgoing gRPC request.
///
/// NOTE(hchen): Compared to `ClientUnaryCall`, this abstract interface doesn't use
/// template. This allows the users (e.g., `ClientCallMangager`) not having to use
/// template as well.
class ClientCall {
 public:
  /// The callback to be called by `ClientCallManager` when the reply of this request is
  /// received.
  virtual void OnReplyReceived() = 0;
  /// Return status.
  virtual ray::Status GetStatus() = 0;
  /// Set return status.
  virtual void SetReturnStatus() = 0;

  virtual ~ClientCall() = default;
};


/// This class wraps a `ClientCall`, and is used as the `tag` of gRPC's `CompletionQueue`.
///
/// The lifecycle of a `ClientCallTag` is as follows.
///
/// When a client submits a new gRPC request, a new `ClientCallTag` object will be created
/// by `ClientCallMangager::CreateCall`. Then the object will be used as the tag of
/// `CompletionQueue`.
///
/// When the reply is received, `ClientCallMangager` will get the address of this object
/// via `CompletionQueue`'s tag. And the manager should call
/// `GetCall()->OnReplyReceived()` and then delete this object.
class ClientCallTag {
 public:
  /// Constructor.
  ///
  /// \param call A `ClientCall` that represents a request.
  explicit ClientCallTag(std::shared_ptr<ClientCall> call,
      bool is_reply = false) : call_(std::move(call)), is_reply_(is_reply) {}

  /// Get the wrapped `ClientCall`.
  const std::shared_ptr<ClientCall> &GetCall() const { return call_; }

  /// Whether this tag corresponds to a reply.
  bool IsReply() const { return is_reply_; }

 private:
  std::shared_ptr<ClientCall> call_;
  /// Whether this tag refers to a request or reply.
  bool is_reply_;
};

class ClientCallManager;

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

/// Implementation of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
///
/// \tparam Reply Type of the Reply message.
template <class Reply>
class ClientUnaryCall : public ClientCall {
 public:
  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientUnaryCall(const ClientCallback<Reply> &callback) : callback_(callback) {}

  Status GetStatus() override {
    absl::MutexLock lock(&mutex_);
    return return_status_;
  }

  void SetReturnStatus() override {
    absl::MutexLock lock(&mutex_);
    return_status_ = GrpcStatusToRayStatus(status_);
  }

  void OnReplyReceived() override {
    ray::Status status;
    {
      absl::MutexLock lock(&mutex_);
      status = return_status_;
    }
    if (callback_ != nullptr) {
      callback_(status, reply_);
    }
  }

 private:
  /// The reply message.
  Reply reply_;

  /// The callback function to handle the reply.
  ClientCallback<Reply> callback_;

  /// The response reader.
  std::unique_ptr<grpc_impl::ClientAsyncResponseReader<Reply>> response_reader_;

  /// gRPC status of this request.
  grpc::Status status_;

  /// Mutex to protect the return_status_ field.
  absl::Mutex mutex_;

  /// This is the status to be returned from GetStatus(). It is safe
  /// to read from other threads while they hold mutex_. We have
  /// return_status_ = GrpcStatusToRayStatus(status_) but need
  /// a separate variable because status_ is set internally by
  /// GRPC and we cannot control it holding the lock.
  ray::Status return_status_ GUARDED_BY(mutex_);

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;

  friend class UnaryCallManager;
};

/// Represents the generic signature of a `FooService::Stub::PrepareAsyncBar`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using PrepareAsyncFunction =
    std::unique_ptr<grpc_impl::ClientAsyncResponseReader<Reply>> (GrpcService::Stub::*)(
        grpc::ClientContext *context, const Request &request, grpc::CompletionQueue *cq);

/// `UnaryCallManager` is used to manage outgoing gRPC requests and the lifecycles of
/// `ClientCall` objects.
///
/// It maintains a thread that keeps polling events from `CompletionQueue`, and post
/// the callback function to the main event loop when a reply is received.
class UnaryCallManager {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which the callback functions will be
  /// posted.
  explicit UnaryCallManager(boost::asio::io_service &main_service, int num_threads = 1)
      : main_service_(main_service), num_threads_(num_threads), shutdown_(false) {
    rr_index_ = rand() % num_threads_;
    // Start the polling threads.
    cqs_.reserve(num_threads_);
    for (int i = 0; i < num_threads_; i++) {
      cqs_.emplace_back();
      polling_threads_.emplace_back(&UnaryCallManager::PollEventsFromCompletionQueue,
                                    this, i);
    }
  }

  ~UnaryCallManager() {
    shutdown_ = true;
    for (auto &cq : cqs_) {
      cq.Shutdown();
    }
    for (auto &polling_thread : polling_threads_) {
      polling_thread.join();
    }
  }

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return A `ClientCall` representing the request that was just sent.
  template <class GrpcService, class Request, class Reply>
  std::shared_ptr<ClientCall> CreateCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {
    auto call = std::make_shared<ClientUnaryCall<Reply>>(callback);
    // Send request.
    // Find the next completion queue to wait for response.
    call->response_reader_ = (stub.*prepare_async_function)(
        &call->context_, request, &cqs_[rr_index_++ % num_threads_]);
    call->response_reader_->StartCall();
    // Create a new tag object. This object will eventually be deleted in the
    // `ClientCallManager::PollEventsFromCompletionQueue` when reply is received.
    //
    // NOTE(chen): Unlike `ServerCall`, we can't directly use `ClientCall` as the tag.
    // Because this function must return a `shared_ptr` to make sure the returned
    // `ClientCall` is safe to use. But `response_reader_->Finish` only accepts a raw
    // pointer.
    auto tag = new ClientCallTag(call);
    call->response_reader_->Finish(&call->reply_, &call->status_, (void *)tag);
    return call;
  }

 private:
  /// This function runs in a background thread. It keeps polling events from the
  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`
  /// objects.
  void PollEventsFromCompletionQueue(int index) {
    void *got_tag;
    bool ok = false;
    // Keep reading events from the `CompletionQueue` until it's shutdown.
    // NOTE(edoakes): we use AsyncNext here because for some unknown reason,
    // synchronous cq_.Next blocks indefinitely in the case that the process
    // received a SIGTERM.
    while (true) {
      auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                   gpr_time_from_millis(250, GPR_TIMESPAN));
      auto status = cqs_[index].AsyncNext(&got_tag, &ok, deadline);
      if (status == grpc::CompletionQueue::SHUTDOWN) {
        break;
      } else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
        // If we timed out and shutdown, then exit immediately. This should not
        // be needed, but gRPC seems to not return SHUTDOWN correctly in these
        // cases (e.g., test_wait will hang on shutdown without this check).
        break;
      } else if (status != grpc::CompletionQueue::TIMEOUT) {
        // std::cout << "received unary reply" << std::endl;
        auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
        tag->GetCall()->SetReturnStatus();
        if (ok && !main_service_.stopped() && !shutdown_) {
          // Post the callback to the main event loop.
          main_service_.post([tag]() {
            tag->GetCall()->OnReplyReceived();
            // The call is finished, and we can delete this tag now.
            delete tag;
          });
        } else {
          delete tag;
        }
      }
    }
  }

  /// The main event loop, to which the callback functions will be posted.
  boost::asio::io_service &main_service_;

  /// The number of polling threads.
  int num_threads_;

  /// Whether the client has shutdown.
  std::atomic<bool> shutdown_;

  /// The index to send RPCs in a round-robin fashion
  std::atomic<unsigned int> rr_index_;

  /// The gRPC `CompletionQueue` object used to poll events.
  std::vector<grpc::CompletionQueue> cqs_;

  /// Polling threads to check the completion queue.
  std::vector<std::thread> polling_threads_;
};

/// Represents the client callback function of a particular rpc method.
///
/// \tparam Reply Type of the reply message.
template <class Reply>
using ClientCallback = std::function<void(const Status &status, const Reply &reply)>;

class ClientStreamCall : public ClientCall {
 public:
  /// Represents state of a `ClientStreamCall`.
  enum class CallState {
    /// Connecting to remote end.
    CONNECTING,
    /// A request in the stream has been writen.
    WRITING,
    /// Finished writing of this stream.
    FINISHED,
  };

  virtual CallState GetState() = 0;

  virtual void SetState(const CallState &new_state) = 0;

  virtual void ReadNextReply() = 0;

  virtual void WriteNextRequest() = 0;

  virtual void SetRequestTag(ClientCallTag *tag) = 0;

  virtual void SetReplyTag(ClientCallTag *tag) = 0;
};


/// Implementation of the `ClientCall`. It represents a `ClientCall` for a particular
/// RPC method.
///
/// \tparam Reply Type of the Reply message.
template <class Request, class Reply>
class ClientStreamCallImpl : public ClientStreamCall {
 public:

  /// Constructor.
  ///
  /// \param[in] callback The callback function to handle the reply.
  explicit ClientStreamCallImpl(boost::asio::io_service &io_service)
    : io_service_(io_service),
      state_(ClientStreamCall::CallState::CONNECTING),
      reply_(std::make_shared<Reply>()),
      // NOTE this flag will be set to true when WriteNextRequest is
      // called when a tag with CONNECT state is received from cq.
      // Set it to false initially as we need to make sure the first tag
      // is received before writing to cq.
      // ready_to_write_(false),
      stream_writer_([this] (const Request &request){
          stream_->Write(request, reinterpret_cast<void *>(tag_)); 
      }),
      start_(std::chrono::system_clock::now()),
      end_(std::chrono::system_clock::now()) {}

  Status GetStatus() override {
    absl::MutexLock lock(&mutex_);
    return return_status_;
  }

  void SetReturnStatus() override {
    absl::MutexLock lock(&mutex_);
    return_status_ = GrpcStatusToRayStatus(status_);
  }

  CallState GetState() override { return state_; }

  void SetState(const CallState &new_state) override { state_ = new_state; }

  void OnReplyReceived() override {
    ray::Status status;
    {
      absl::MutexLock lock(&mutex_);
      status = return_status_;
    }

    std::shared_ptr<Reply> reply = std::make_shared<Reply>();
    std::swap(reply, reply_);

    // TODO(zhijunfu): this callback needs to be per request.
    // To track request and reply, we can require Request & Reply
    // to have an identifier in order to match.
    // Also reply is required to have an status.
    ClientCallback<Reply> reply_callback;
    {
      absl::MutexLock lock(&mutex_);
      auto iter = pending_callbacks_.find(reply->request_id());
      if (iter != pending_callbacks_.end()) {
        // std::cout << "matched reply " << reply->request_id() << std::endl;
        reply_callback = iter->second;
        pending_callbacks_.erase(iter);
      }
    }

    if (reply_callback != nullptr) {
      io_service_.post([reply_callback, reply] {
        reply_callback(Status::OK(), *reply);
      });
    }

    ReadNextReply();
  }

  void ReadNextReply() override {
    stream_->Read(reply_.get(), reinterpret_cast<void *>(reply_tag_));
  }

  void WriteRequest(std::shared_ptr<Request> request,
      const ClientCallback<Reply> &callback) {

    {
      absl::MutexLock lock(&mutex_);
      pending_callbacks_.emplace(std::make_pair(request->request_id(), callback));
    }

    stream_writer_.WriteStream(request);
  }

  void WriteNextRequest() override {
    stream_writer_.OnStreamWritten();
  }

  void SetRequestTag(ClientCallTag *tag) override { tag_ = tag; }

  void SetReplyTag(ClientCallTag *tag) override { reply_tag_ = tag; }

 private:

  /// The io service to process replies.
  boost::asio::io_service &io_service_;

  CallState state_;

  std::shared_ptr<Reply> reply_;

  /// Async reader writer.
  std::unique_ptr<grpc_impl::ClientAsyncReaderWriter<Request, Reply>> stream_;

  /// Tag for stream request.
  ClientCallTag *tag_;

  /// Tag for stream reply.
  ClientCallTag *reply_tag_;

  StreamWriter<Request> stream_writer_;

  /// Map from request id to the corresponding reply callback, which will be
  /// invoked when the reply is received for the request.
  std::unordered_map<uint64_t, ClientCallback<Reply>> pending_callbacks_ GUARDED_BY(mutex_);

  /// gRPC status of this request.
  grpc::Status status_;

  /// Mutex to protect the return_status_ field.
  absl::Mutex mutex_;

  /// This is the status to be returned from GetStatus(). It is safe
  /// to read from other threads while they hold mutex_. We have
  /// return_status_ = GrpcStatusToRayStatus(status_) but need
  /// a separate variable because status_ is set internally by
  /// GRPC and we cannot control it holding the lock.
  ray::Status return_status_ GUARDED_BY(mutex_);

  /// Context for the client. It could be used to convey extra information to
  /// the server and/or tweak certain RPC behaviors.
  grpc::ClientContext context_;

  friend class StreamCallManager;

  ///
  std::chrono::system_clock::time_point start_;
  std::chrono::system_clock::time_point end_;
};

/// Represents the generic signature of a `FooService::Stub::PrepareAsyncBar`
/// function, where `Foo` is the service name and `Bar` is the rpc method name.
///
/// \tparam GrpcService Type of the gRPC-generated service class.
/// \tparam Request Type of the request message.
/// \tparam Reply Type of the reply message.
template <class GrpcService, class Request, class Reply>
using PrepareAsyncStreamFunction =
    std::unique_ptr<grpc_impl::ClientAsyncReaderWriter<Request, Reply>> (GrpcService::Stub::*)(
        grpc::ClientContext *context, grpc::CompletionQueue *cq);

/// `ClientCallManager` is used to manage outgoing gRPC requests and the lifecycles of
/// `ClientCall` objects.
///
/// It maintains a thread that keeps polling events from `CompletionQueue`, and post
/// the callback function to the main event loop when a reply is received.
///
/// Multiple clients can share one `ClientCallManager`.
class StreamCallManager {
 public:
  /// Constructor.
  ///
  /// \param[in] main_service The main event loop, to which the callback functions will be
  /// posted.
  explicit StreamCallManager(boost::asio::io_service &main_service)
      : main_service_(main_service), shutdown_(false) {
    polling_thread_ = std::thread(&StreamCallManager::PollEventsFromCompletionQueue,
                                   this);
  }

  ~StreamCallManager() {
    shutdown_ = true;
    cq_.Shutdown();
    polling_thread_.join();
  }

  /// Create a new `ClientCall` and send request.
  ///
  /// \tparam GrpcService Type of the gRPC-generated service class.
  /// \tparam Request Type of the request message.
  /// \tparam Reply Type of the reply message.
  ///
  /// \param[in] stub The gRPC-generated stub.
  /// \param[in] prepare_async_function Pointer to the gRPC-generated
  /// `FooService::Stub::PrepareAsyncBar` function.
  /// \param[in] request The request message.
  /// \param[in] callback The callback function that handles reply.
  ///
  /// \return A `ClientCall` representing the request that was just sent.
  template <class GrpcService, class Request, class Reply>
  std::shared_ptr<ClientStreamCallImpl<Request, Reply>> CreateCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncStreamFunction<GrpcService, Request, Reply> prepare_async_function) {
    auto call = std::make_shared<ClientStreamCallImpl<Request, Reply>>(main_service_);
    auto tag = new ClientCallTag(call, /*is_reply=*/false);
    auto reply_tag = new ClientCallTag(call, /*is_reply=*/true);
    call->SetRequestTag(tag);
    call->SetReplyTag(reply_tag);
    // Send request.
    // Find the next completion queue to wait for response.
    call->stream_ = (stub.*prepare_async_function)(
        &call->context_, &cq_);
    call->stream_->StartCall((void*)(tag));

    return call;
  }

 private:
  /// This function runs in a background thread. It keeps polling events from the
  /// `CompletionQueue`, and dispatches the event to the callbacks via the `ClientCall`
  /// objects.
  void PollEventsFromCompletionQueue() {
    void *got_tag;
    bool ok = false;
    // Keep reading events from the `CompletionQueue` until it's shutdown.
    // NOTE(edoakes): we use AsyncNext here because for some unknown reason,
    // synchronous cq_.Next blocks indefinitely in the case that the process
    // received a SIGTERM.
    while (true) {
      auto deadline = gpr_time_add(gpr_now(GPR_CLOCK_REALTIME),
                                   gpr_time_from_millis(250, GPR_TIMESPAN));
      auto status = cq_.AsyncNext(&got_tag, &ok, deadline);
      if (status == grpc::CompletionQueue::SHUTDOWN) {
        break;
      } else if (status == grpc::CompletionQueue::TIMEOUT && shutdown_) {
        // If we timed out and shutdown, then exit immediately. This should not
        // be needed, but gRPC seems to not return SHUTDOWN correctly in these
        // cases (e.g., test_wait will hang on shutdown without this check).
        break;
      } else if (status != grpc::CompletionQueue::TIMEOUT) {
        // std::cout << "received stream reply" << std::endl;
        auto tag = reinterpret_cast<ClientCallTag *>(got_tag);
        tag->GetCall()->SetReturnStatus();
        if (ok && !main_service_.stopped() && !shutdown_) {
          if (tag->IsReply()) {
            tag->GetCall()->OnReplyReceived();
          } else {
            auto call = tag->GetCall();
            ClientStreamCall *stream_call =
                dynamic_cast<ClientStreamCall *>(call.get());
            switch (stream_call->GetState()) {
            case ClientStreamCall::CallState::CONNECTING:
              stream_call->ReadNextReply();
              stream_call->SetState(ClientStreamCall::CallState::WRITING);
              stream_call->WriteNextRequest();
              break;

            case ClientStreamCall::CallState::WRITING:
              // A write has finished, it's ok to write the next request.
              stream_call->WriteNextRequest();
              break;
            default:
              RAY_LOG(FATAL) << "Invalid stream call state "
                             << static_cast<int>(stream_call->GetState());
            } // switch
          } 
        }
      }
    }
  }

  /// The main event loop, to which the callback functions will be posted.
  boost::asio::io_service &main_service_;

  /// Whether the client has shutdown.
  std::atomic<bool> shutdown_;

  /// The gRPC `CompletionQueue` object used to poll events.
  grpc::CompletionQueue cq_;

  /// Polling threads to check the completion queue.
  std::thread polling_thread_;
};

/// `ClientCallManager` is used to manage outgoing gRPC requests and the lifecycles of
/// `ClientCall` objects.
///
/// It maintains a thread that keeps polling events from `CompletionQueue`, and post
/// the callback function to the main event loop when a reply is received.
///
/// Multiple clients can share one `ClientCallManager`.
class ClientCallManager {
 public:
  explicit ClientCallManager(boost::asio::io_service &main_service, int num_threads = 1)
    : unary_call_manager_(main_service, num_threads),
      stream_call_manager_(main_service) {}
    
  
  template <class GrpcService, class Request, class Reply>
  std::shared_ptr<ClientCall> CreateCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncFunction<GrpcService, Request, Reply> prepare_async_function,
      const Request &request, const ClientCallback<Reply> &callback) {
    return unary_call_manager_.CreateCall<GrpcService, Request, Reply>(
        stub, prepare_async_function, request, callback);
  }

 template <class GrpcService, class Request, class Reply>
  std::shared_ptr<ClientStreamCallImpl<Request, Reply>> CreateStreamCall(
      typename GrpcService::Stub &stub,
      const PrepareAsyncStreamFunction<GrpcService, Request, Reply> prepare_async_function) {
    return stream_call_manager_.CreateCall<GrpcService, Request, Reply>(
        stub, prepare_async_function);
  }

 private:
  UnaryCallManager unary_call_manager_;
  StreamCallManager stream_call_manager_;

};

}  // namespace rpc
}  // namespace ray

#endif
