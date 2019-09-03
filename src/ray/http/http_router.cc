#include "ray/http/http_router.h"
#include "ray/http/http_server.h"

namespace ray {
HttpReply::~HttpReply() {
  response_.prepare_payload();
  session_->Reply(std::move(response_));
  session_.reset();
}

boost::asio::io_context::executor_type HttpReply::GetExecutor() {
  return session_->GetExecutor();
}
}  // namespace ray
