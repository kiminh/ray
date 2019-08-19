#pragma once

#include <boost/asio/ip/detail/endpoint.hpp>
namespace net = boost::asio;

namespace ray {
namespace gcs {

class Failover {
 public:
  virtual ~Failover() = default;

  virtual void OnWorkerDisconnected(const uint64_t &node_id,
                                    const net::ip::detail::endpoint &ep) = 0;

  virtual void OnWorkerConnected(const uint64_t &node_id,
                                 const net::ip::detail::endpoint &ep) = 0;

  virtual void OnWorkerRestarted(const uint64_t &node_id,
                                 const net::ip::detail::endpoint &ep) = 0;
};

}  // namespace gcs
}  // namespace ray
