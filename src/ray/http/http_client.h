#pragma once

#include <boost/asio.hpp>
#include <boost/beast.hpp>
#include <boost/bind.hpp>
#include "ray/util/logging.h"

#include <list>

using tcp = boost::asio::ip::tcp;
namespace beast = boost::beast;
namespace http = beast::http;

namespace ray {
class HttpSyncClient {
 public:
  HttpSyncClient() : socket_(ioc_) {}

  ~HttpSyncClient() { Close(); }

  bool Connect(const std::string &host, int port) {
    if (is_connected_) {
      return true;
    }

    host_ = host;
    tcp::resolver resolver{ioc_};
    auto results = resolver.resolve(host, std::to_string(port));
    boost::asio::connect(socket_, results.begin(), results.end());
    is_connected_ = true;
    return true;
  }

  bool IsConnected() const { return is_connected_; }

  void Close() {
    if (!is_connected_) {
      return;
    }

    // Gracefully close the socket
    boost::system::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    if (ec && ec != boost::system::errc::not_connected) {
      // TODO(hc):
    }
    is_connected_ = false;
  }

  std::string Get(const std::string &uri,
                  const std::unordered_map<std::string, std::string> &params) {
    std::ostringstream ostr;
    ostr << uri;
    auto itr = params.begin();
    if (itr != params.end()) {
      ostr << "?";
    }
    for (; itr != params.end();) {
      ostr << itr->first;
      if (!itr->second.empty()) {
        ostr << "=" << itr->second;
      }
      if (++itr != params.end()) {
        ostr << "&";
      }
    }
    std::string target = ostr.str();

    http::request<http::string_body> req{http::verb::get, target, 11};
    req.set(http::field::host, host_);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);

    // Send the HTTP request to the remote host
    http::write(socket_, req);

    // This buffer is used for reading and must be persisted
    beast::flat_buffer buffer;

    // Declare a container to hold the response
    http::response<http::string_body> res;

    // Receive the HTTP response
    http::read(socket_, buffer, res);

    return res.body();
  }

  std::string Post(const std::string &uri,
                   const std::unordered_map<std::string, std::string> &params,
                   std::string &&data) {
    std::ostringstream ostr;
    ostr << uri;
    auto itr = params.begin();
    if (itr != params.end()) {
      ostr << "?";
    }
    for (; itr != params.end();) {
      ostr << itr->first;
      if (!itr->second.empty()) {
        ostr << "=" << itr->second;
      }
      if (++itr != params.end()) {
        ostr << "&";
      }
    }
    std::string target = ostr.str();

    http::request<http::string_body> req{http::verb::post, target, 11};
    req.set(http::field::host, host_);
    req.set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req.set(http::field::content_length, data.size());
    req.body() = std::move(data);
    req.prepare_payload();

    // Send the HTTP request to the remote host
    http::write(socket_, req);

    // This buffer is used for reading and must be persisted
    beast::flat_buffer buffer;

    // Declare a container to hold the response
    http::response<http::string_body> res;

    // Receive the HTTP response
    http::read(socket_, buffer, res);

    return res.body();
  }

 private:
  boost::asio::io_context ioc_;
  tcp::socket socket_;
  std::string host_;
  bool is_connected_ = false;
};

class HttpAsyncClient : public std::enable_shared_from_this<HttpAsyncClient> {
  typedef std::function<void(boost::system::error_code ec, const std::string &&)>
      Callback;

 public:
  HttpAsyncClient(boost::asio::io_context &ioc) : ioc_(ioc), socket_(ioc) {}

  ~HttpAsyncClient() { Close(); }

  bool Connect(const std::string &host, int port) {
    if (is_connected_) {
      return true;
    }

    host_ = host;
    tcp::resolver resolver{ioc_};
    auto results = resolver.resolve(host, std::to_string(port));
    boost::asio::connect(socket_, results.begin(), results.end());
    is_connected_ = true;
    is_working_ = false;
    return true;
  }

  bool IsConnected() const { return is_connected_; }

  void Close() {
    if (!is_connected_) {
      return;
    }

    // Gracefully close the socket
    boost::system::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    if (ec && ec != boost::system::errc::not_connected) {
      // TODO(hc):
    }
    is_connected_ = false;
    is_working_ = false;

    // Notify all callbacks in queue
    const boost::system::error_code cancel_ec =
        boost::system::errc::make_error_code(boost::system::errc::operation_canceled);
    for (auto &i : queue_) {
      i.second(cancel_ec, std::string());
    }
    queue_.clear();
  }

  void Get(const std::string &uri,
           const std::unordered_map<std::string, std::string> &params,
           Callback &&callback) {
    if (!is_connected_) {
      callback(boost::system::errc::make_error_code(boost::system::errc::not_connected),
               std::string());
      return;
    }

    std::ostringstream ostr;
    ostr << uri;
    auto itr = params.begin();
    if (itr != params.end()) {
      ostr << "?";
    }
    for (; itr != params.end();) {
      ostr << itr->first;
      if (!itr->second.empty()) {
        ostr << "=" << itr->second;
      }
      if (++itr != params.end()) {
        ostr << "&";
      }
    }
    std::string target = ostr.str();

    auto req = std::make_shared<http::request<http::string_body>>();
    req->version(11);
    req->method(http::verb::get);
    req->target(target);
    req->set(http::field::host, host_);
    req->set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);

    queue_.emplace_back(std::make_pair(req, callback));
    exec_one();
  }

  void Post(const std::string &uri,
            const std::unordered_map<std::string, std::string> &params,
            std::string &&data, Callback &&callback) {
    if (!is_connected_) {
      callback(boost::system::errc::make_error_code(boost::system::errc::not_connected),
               std::string());
      return;
    }

    std::ostringstream ostr;
    ostr << uri;
    auto itr = params.begin();
    if (itr != params.end()) {
      ostr << "?";
    }
    for (; itr != params.end();) {
      ostr << itr->first;
      if (!itr->second.empty()) {
        ostr << "=" << itr->second;
      }
      if (++itr != params.end()) {
        ostr << "&";
      }
    }
    std::string target = ostr.str();

    auto req = std::make_shared<http::request<http::string_body>>();
    req->version(11);
    req->method(http::verb::post);
    req->target(target);
    req->set(http::field::host, host_);
    req->set(http::field::user_agent, BOOST_BEAST_VERSION_STRING);
    req->set(http::field::content_length, data.size());
    req->body() = std::move(data);
    req->prepare_payload();

    queue_.emplace_back(std::make_pair(req, callback));
    exec_one();
  }

 private:
  void on_write(boost::system::error_code ec, std::size_t bytes_transferred,
                std::shared_ptr<http::request<http::string_body>> req,
                Callback callback) {
    boost::ignore_unused(bytes_transferred);
    boost::ignore_unused(req);
    if (ec) {
      RAY_LOG(ERROR) << "on_write " << host_ << " failed, err: " << ec.message();
      Close();
      return;
    }

    auto res = std::make_shared<http::response<http::string_body>>();
    auto buffer = std::make_shared<beast::flat_buffer>();

    http::async_read(
        socket_, *buffer, *res,
        boost::bind(&HttpAsyncClient::on_read, shared_from_this(),
                    boost::asio::placeholders::error,
                    boost::asio::placeholders::bytes_transferred, res, buffer, callback));
  }

  void on_read(boost::system::error_code ec, std::size_t bytes_transferred,
               std::shared_ptr<http::response<http::string_body>> res,
               std::shared_ptr<beast::flat_buffer> buffer, Callback callback) {
    boost::ignore_unused(bytes_transferred);
    boost::ignore_unused(buffer);
    if (ec) {
      RAY_LOG(ERROR) << "on_read " << host_ << " failed, err: " << ec.message();
      Close();
      return;
    }

    callback(ec, std::move(res->body()));
    is_working_ = false;
    exec_one();
  }

  void exec_one() {
    if (is_connected_ && !is_working_ && !queue_.empty()) {
      is_working_ = true;
      auto e = queue_.front();
      queue_.pop_front();
      http::async_write(
          socket_, *e.first,
          boost::bind(&HttpAsyncClient::on_write, shared_from_this(),
                      boost::asio::placeholders::error,
                      boost::asio::placeholders::bytes_transferred, e.first, e.second));
    }
  }

 private:
  boost::asio::io_context &ioc_;
  std::string host_;
  tcp::socket socket_;
  bool is_connected_ = false;
  bool is_working_ = false;
  std::list<std::pair<std::shared_ptr<http::request<http::string_body>>, Callback>>
      queue_;
};
}  // namespace ray
