#include "failure_detector_slave.h"
#include <fstream>
#include "ray/util/json.h"

namespace ray {
namespace fd {
FailureDetectorSlave::FailureDetectorSlave(boost::asio::io_context &ioc)
    : FailureDetector(ioc) {}

void FailureDetectorSlave::Run(const std::string &json_config_file,
                               const ip::detail::endpoint &target, uint32_t delay_ms) {
  std::stringstream ss;
  std::ifstream file(json_config_file);
  if (file) {
    ss << file.rdbuf();
    file.close();
  } else {
    throw std::runtime_error("!! Unable to open json file");
  }

  rapidjson::Document doc;
  if (doc.Parse<0>(ss.str().c_str()).HasParseError()) {
    throw std::invalid_argument("json parse error");
  }

  uint32_t check_interval_seconds = doc["fd"]["check_interval_seconds"].GetUint();
  uint32_t beacon_interval_seconds = doc["fd"]["beacon_interval_seconds"].GetUint();
  uint32_t lease_seconds = doc["fd"]["lease_seconds"].GetUint();
  uint32_t grace_seconds = doc["fd"]["grace_seconds"].GetUint();
  FailureDetector::Start(check_interval_seconds, beacon_interval_seconds, lease_seconds,
                         grace_seconds);

  RegisterMaster(target, delay_ms);
}

}  // namespace fd
}  // namespace ray