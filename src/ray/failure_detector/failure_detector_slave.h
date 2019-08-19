#pragma once

#include <boost/asio.hpp>
#include "failure_detector.h"

namespace ray {
namespace fd {

class FailureDetectorSlave : public FailureDetector {
 public:
  explicit FailureDetectorSlave(boost::asio::io_context &ioc);

  void Run(const std::string &json_config_file, const ip::detail::endpoint &target,
           uint32_t delay_ms);
};

}  // namespace fd
}  // namespace ray
