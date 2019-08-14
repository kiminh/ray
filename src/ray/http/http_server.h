//
// Copyright (c) 2016-2017 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//
// Official repository: https://github.com/boostorg/beast
//

//------------------------------------------------------------------------------
//
// Example: HTTP server, asynchronous
//
//------------------------------------------------------------------------------
#pragma once

#include <algorithm>
#include <boost/asio/bind_executor.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>
#include <boost/config.hpp>
#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "http_router.h"
#include "ray/util/logging.h"

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;
namespace http = beast::http;

namespace ray {
// Handles an HTTP server connection
class Session : public std::enable_shared_from_this<Session> {
  // This is the C++11 equivalent of a generic lambda.
  // The function object is used to send an HTTP message.
  struct SendLambda {
    explicit SendLambda(Session &self) : self_(self) {}

    template <bool isRequest, class Body, class Fields>
    void operator()(http::message<isRequest, Body, Fields> &&msg) const {
      // The lifetime of the message has to extend
      // for the duration of the async operation so
      // we use a shared_ptr to manage it.
      auto sp = std::make_shared<http::message<isRequest, Body, Fields>>(std::move(msg));

      // Store a type-erased version of the shared
      // pointer in the class to keep it alive.
      self_.res_ = sp;

      // Write the response
      http::async_write(
          self_.socket_, *sp,
          boost::asio::bind_executor(
              self_.strand_,
              std::bind(&Session::OnWrite, self_.shared_from_this(),
                        std::placeholders::_1, std::placeholders::_2, sp->need_eof())));
    }

    Session &self_;
  };

 public:
  // Take ownership of the socket
  explicit Session(tcp::socket socket)
      : socket_(std::move(socket)), strand_(socket_.get_executor()), lambda_(*this) {}

  // Start the asynchronous operation
  void Run() { DoRead(); }

 private:
  void DoRead() {
    // Make the request empty before reading,
    // otherwise the operation behavior is undefined.
    req_ = {};

    // Read a request
    http::async_read(
        socket_, buffer_, req_,
        boost::asio::bind_executor(
            strand_, std::bind(&Session::OnRead, shared_from_this(),
                               std::placeholders::_1, std::placeholders::_2)));
  }

  void OnRead(beast::error_code ec, std::size_t bytes_transferred) {
    boost::ignore_unused(bytes_transferred);

    // This means they closed the connection
    if (ec == http::error::end_of_stream) {
      return DoClose();
    }

    if (ec) {
      RAY_LOG(ERROR) << "read failed, " << ec.message();
      return DoClose();
    }

    lambda_(HttpRouter::Route(std::move(req_)));
  }

  void OnWrite(beast::error_code ec, std::size_t bytes_transferred, bool close) {
    boost::ignore_unused(bytes_transferred);
    if (ec) {
      RAY_LOG(ERROR) << "write failed, err: " << ec.message();
      // This session is broken, then close it directly
      return DoClose();
    }

    if (close) {
      // This means we should close the connection, usually because
      // the response indicated the "Connection: close" semantic.
      return DoClose();
    }

    // We're done with the response so delete it
    res_ = nullptr;

    // Read another request
    DoRead();
  }

  void DoClose() {
    // Send a TCP shutdown
    beast::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_send, ec);
    // At this point the connection is closed gracefully
  }

 private:
  tcp::socket socket_;
  boost::asio::strand<boost::asio::io_context::executor_type> strand_;
  boost::beast::flat_buffer buffer_;
  http::request<http::string_body> req_;
  std::shared_ptr<void> res_;
  SendLambda lambda_;
};

// Accepts incoming connections and launches the sessions
class HttpServer : public std::enable_shared_from_this<HttpServer> {
 public:
  explicit HttpServer(boost::asio::io_context &ioc) : acceptor_(ioc), socket_(ioc) {}

  void Start(const tcp::endpoint &endpoint) {
    Init(endpoint);
    DoAccept();
  }

  void Start(const std::string &host, uint16_t port) {
    Init(tcp::endpoint(boost::asio::ip::make_address(host), port));
    DoAccept();
  }

  int32_t Port() const { return acceptor_.local_endpoint().port(); }

 private:
  void Init(const tcp::endpoint &endpoint) {
    beast::error_code ec;

    // Open the acceptor
    acceptor_.open(endpoint.protocol(), ec);
    RAY_CHECK(!ec) << "open failed"
                   << ", ep: " << endpoint << ", err: " << ec.message();

    acceptor_.set_option(boost::asio::socket_base::reuse_address(true), ec);
    RAY_CHECK(!ec) << "reuse address failed"
                   << ", ep: " << endpoint << ", err: " << ec.message();

    // Bind to the server address
    acceptor_.bind(endpoint, ec);
    RAY_CHECK(!ec) << "bind failed "
                   << ", ep: " << endpoint << ", err: " << ec.message();

    // Start listening for connections
    acceptor_.listen(boost::asio::socket_base::max_listen_connections, ec);
    RAY_CHECK(!ec) << "listen failed"
                   << ", ep: " << endpoint << ", err: " << ec.message();
  }

  void DoAccept() {
    acceptor_.async_accept(socket_, std::bind(&HttpServer::OnAccept, shared_from_this(),
                                              std::placeholders::_1));
  }

  void OnAccept(beast::error_code ec) {
    if (ec) {
      RAY_LOG(ERROR) << "accept failed"
                     << ", err: " << ec.message();
    } else {
      // Create the session and run it
      std::make_shared<Session>(std::move(socket_))->Run();
    }
    // Accept another connection
    DoAccept();
  }

 private:
  tcp::acceptor acceptor_;
  tcp::socket socket_;
};
}  // namespace ray
