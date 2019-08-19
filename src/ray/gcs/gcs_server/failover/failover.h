#pragma once

#include <boost/asio.hpp>
namespace net = boost::asio;

namespace ray {

namespace gcs {

class Failover {
 public:
  virtual ~Failover() = default;

  virtual void OnWorkerDisconnected(
      const std::string &node_id, const net::ip::detail::endpoint &ep) = 0;

  virtual void OnWorkerConnected(
      const std::string &node_id, const net::ip::detail::endpoint &ep) = 0;

  virtual void OnWorkerRestarted(
      const std::string &node_id, const net::ip::detail::endpoint &ep) = 0;
};

}

}
