#pragma once

#include <boost/asio.hpp>
#include <boost/beast.hpp>

using tcp = boost::asio::ip::tcp;
namespace http = boost::beast::http;

class HttpSyncClient {
 public:
  HttpSyncClient() : socket_(ioc_) {
  }

  ~HttpSyncClient() {
    Close();
  }

  bool Connect(const std::string& host, int port) {
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

  void Close() {
    if (!is_connected_) {
      return;
    }

    // Gracefully close the socket
    boost::system::error_code ec;
    socket_.shutdown(tcp::socket::shutdown_both, ec);
    if(ec && ec != boost::system::errc::not_connected) {
      // TODO(hc):
    }
    is_connected_ = true;
  }

  std::string Get(const std::string& uri, const std::unordered_map<std::string, std::string>& params) {
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
    boost::beast::flat_buffer buffer;

    // Declare a container to hold the response
    http::response<http::string_body> res;

    // Receive the HTTP response
    http::read(socket_, buffer, res);

    return res.body();
  }

  std::string Post(const std::string& uri, const std::unordered_map<std::string, std::string>& params, std::string&& data) {
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
    boost::beast::flat_buffer buffer;

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
