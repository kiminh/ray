#pragma once

#include "ray/protobuf/gcs_server.pb.h"
#include "failover/abstract_failover.h"
#include "ray/failure_detector/failure_detector.h"

#include <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;

using FailureDetect = ray::failure_detector::FailureDetector;

namespace ray {

namespace gcs {

class GcsServer {
 public:
  explicit GcsServer(boost::asio::io_context &ioc);

 public:
  void Start(const tcp::endpoint &ep);

  void Start(const std::string &host, uint16_t port);

  void OnRegister(const rpc::RegisterRequest& req, rpc::RegisterReply& reply);

 private:
  boost::asio::io_context &ioc_;
  std::shared_ptr<FailureDetect> fd_;
  std::unique_ptr<AbstractFailover> failover_;
};

}

}
