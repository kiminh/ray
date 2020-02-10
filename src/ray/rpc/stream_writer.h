#ifndef RAY_RPC_STREAM_WRITER_H
#define RAY_RPC_STREAM_WRITER_H

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

template <class Request>
using WriteStreamFunction = std::function<void (const Request &)>; 

template <class Request>
class StreamWriter {
 public:
  StreamWriter(WriteStreamFunction<Request> write_stream_func)
    : ready_to_write_(false),
      write_stream_func_(write_stream_func) {}

  void WriteStream(std::shared_ptr<Request> request) {
    write_mutex_.Lock();
    // pending_callbacks_.emplace(std::make_pair(request->request_id(), callback));

    if (ready_to_write_) {
      ready_to_write_ = false;
      write_mutex_.Unlock();

      write_stream_func_(*request);
    } else {
      pending_requests_.emplace(request);
      write_mutex_.Unlock();
    }
  }

  void OnStreamWritten() {

    write_mutex_.Lock();
    if (pending_requests_.empty()) {
      ready_to_write_ = true;
      write_mutex_.Unlock();
    } else {
      ready_to_write_ = false;
      auto request = pending_requests_.front();
      pending_requests_.pop();
      write_mutex_.Unlock();

      write_stream_func_(*request);
    }
  }

 private:
  /// Mutex to protect ready_to_write_ and pending_requests_ fields.
  absl::Mutex write_mutex_;

  /// Whether it's ready to write to this stream.
  bool ready_to_write_ GUARDED_BY(write_mutex_);

  // Buffer for sending requests to the server. We need write the request into the queue
  // when it is not ready to write.
  std::queue<std::shared_ptr<Request>> pending_requests_ GUARDED_BY(write_mutex_);

  WriteStreamFunction<Request> write_stream_func_;
};


}  // namespace rpc
}  // namespace ray

#endif
