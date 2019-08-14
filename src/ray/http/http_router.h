#pragma once

#include <boost/beast/http.hpp>
#include <boost/beast/version.hpp>

#include <boost/thread/locks.hpp>
#include <boost/thread/shared_mutex.hpp>

#include <unordered_map>
#include <functional>
#include <shared_mutex>
#include <vector>
#include <algorithm>

namespace beast = boost::beast;
namespace http = beast::http;

class HttpResult {
 public:
  explicit HttpResult(int code, std::string&& msg)
    : code_(code), msg_(std::move(msg)) {
  }

  HttpResult(HttpResult&& other) noexcept {
    code_ = other.code_;
    msg_ = std::move(other.msg_);
  }

  std::string toJson() const {
    std::ostringstream ostr;
    ostr << R"({"status":{"code":)" << code_ << R"(, "msg":")" << msg_ << "\"}}";
    return ostr.str();
  }

 private:
  int code_ = 0;
  std::string msg_;
};

class HttpReply {
 public:
  explicit HttpReply(http::response<http::string_body>& response)
    : response_(response) {
  }

  void SetJsonContent(std::string&& content) {
    response_.content_length(content.size());
    response_.body() = std::move(content);
    response_.set(http::field::content_type, "application/json");
  }

  void SetJsonContent(const std::string& content) {
    response_.content_length(content.size());
    response_.body() = content;
    response_.set(http::field::content_type, "application/json");
  }

  void SetPlainContent(std::string&& content) {
    response_.content_length(content.size());
    response_.body() = std::move(content);
    response_.set(http::field::content_type, "text/plain");
  }

  void SetPlainContent(const std::string& content) {
    response_.content_length(content.size());
    response_.body() = content;
    response_.set(http::field::content_type, "text/plain");
  }

 private:
  http::response<http::string_body>& response_;
};

typedef std::unordered_map<std::string, std::string> HttpParams;
typedef std::function<void(HttpParams&&, const std::string&, HttpReply&)> HttpHandler;

class HttpRouter {
 public:
  static bool Register(const std::string& uri, HttpHandler&& handler) {
    return HttpRouter::Instance().RegisterHandler(uri, std::move(handler));
  }

  static http::response<http::string_body> Route(http::request<http::string_body> &&req) {
    return HttpRouter::Instance().RouteRequest(std::move(req));
  }

 private:
  static HttpRouter& Instance() {
    static HttpRouter router;
    return router;
  }

  bool RegisterHandler(const std::string& uri, HttpHandler&& handler) {
    boost::unique_lock<boost::shared_mutex> lock(mutex_);
    auto itr = handlers_.find(uri);
    if (itr != handlers_.end()) {
      return false;
    }

    handlers_.emplace(uri, std::move(handler));
    return false;
  }

  std::pair<std::string, std::string> getKv(const std::string& str) {
    std::pair<std::string, std::string> kv;
    auto pos = str.find('=');
    if (pos == std::string::npos) {
      kv.first = str;
    } else {
      kv.first = str.substr(0, pos);
      kv.second = str.substr(pos+1);
    }
    return kv;
  }

  std::unordered_map<std::string, std::string> parseQueryParams(const std::string& query) {
    std::unordered_map<std::string, std::string> params;

    size_t len = query.size();
    std::string::size_type cur1 = 0;
    std::string::size_type cur2 = query.find('&');
    while(std::string::npos != cur2) {
      params.emplace(getKv(query.substr(cur1, cur2-cur1)));
      cur1 = cur2 + 1;
      cur2 = query.find('&', cur1);
    }

    if(cur1 != len) {
      params.emplace(getKv(query.substr(cur1)));
    }

    return params;
  }

  bool validateURL(const std::string& url) {
    for (auto c : url) {
      if (c <= 0x20 || c == 0x7f) {
        // no controls or unescaped spaces
        return false;
      }
    }
    return true;
  }

  bool ParseUrlNonFully(const std::string& url, HttpParams& params, std::string& path) {
    if (!validateURL(url)) {
      return false;
    }

    auto pathStart = url.find('/');
    auto queryStart = url.find('?');
    auto hashStart = url.find('#');

    auto queryEnd = std::min(hashStart, std::string::npos);
    auto pathEnd = std::min(queryStart, hashStart);

    if (pathStart < pathEnd) {
      path = url.substr(pathStart, pathEnd - pathStart);
    } else {
      // missing the '/', e.g. '?query=3'
      path.clear();
    }

    if (queryStart < queryEnd) {
      auto query = url.substr(queryStart + 1, queryEnd - queryStart - 1);
      params = parseQueryParams(query);
    } else if (queryStart != std::string::npos && hashStart < queryStart) {
      return false;
    }

    return true;
  }

  http::response<http::string_body> RouteRequest(http::request<http::string_body> &&req) {
    std::string path;
    HttpParams params;
    if (!ParseUrlNonFully(req.target().to_string(), params, path)) {
      http::response<http::string_body> resp{http::status::bad_request, req.version()};
      HttpReply reply(resp);
      reply.SetJsonContent(HttpResult(400, "bad request!").toJson());
      resp.prepare_payload();
      return resp;
    }

    boost::shared_lock<boost::shared_mutex> lock(mutex_);
    auto itr = handlers_.find(path);
    if (itr == handlers_.end()) {
      http::response<http::string_body> resp{http::status::not_found, req.version()};
      HttpReply reply(resp);
      std::string msg = "uri " + path + " is not registered!";
      reply.SetJsonContent(HttpResult(404, std::move(msg)).toJson());
      resp.prepare_payload();
      return resp;
    }

    http::response<http::string_body> resp{http::status::ok, req.version()};
    resp.set(http::field::server, BOOST_BEAST_VERSION_STRING);
    resp.keep_alive(req.keep_alive());

    HttpReply reply(resp);
    itr->second(std::move(params), req.body(), reply);
    resp.prepare_payload();
    return resp;
  }

 private:
  HttpRouter() = default;
  ~HttpRouter() = default;

 private:
  boost::shared_mutex mutex_{};
  std::unordered_map<std::string, HttpHandler> handlers_;
};