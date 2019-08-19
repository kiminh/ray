#include "gcs_server.h"

#include "ray/common/ray_config.h"
#include "ray/util/util.h"

#include "gflags/gflags.h"
#include "gcs_server.h"
#include "ray/http/http_server.h"

#include <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;

DEFINE_string(redis_address, "", "The ip address of redis.");
DEFINE_int32(redis_port, -1, "The port of redis.");
DEFINE_string(config_list, "", "The config list of raylet.");
DEFINE_string(redis_password, "", "The password of redis.");

namespace ray {

namespace gcs {

int main(int argc, char *argv[]) {
  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog,
                                         argv[0],
                                         ray::RayLogLevel::INFO, /*log_dir=*/
                                         "");
  ray::RayLog::InstallFailureSignalHandler();

  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string redis_address = FLAGS_redis_address;
  const int redis_port = static_cast<int>(FLAGS_redis_port);
  const std::string config_list = FLAGS_config_list;
  const std::string redis_password = FLAGS_redis_password;
  gflags::ShutDownCommandLineFlags();

  std::unordered_map<std::string, std::string> raylet_config;

  // Parse the configuration list.
  std::istringstream config_string(config_list);
  std::string config_name;
  std::string config_value;

  while (std::getline(config_string, config_name, ',')) {
    RAY_CHECK(std::getline(config_string, config_value, ','));
    // TODO(rkn): The line below could throw an exception. What should we do about this?
    raylet_config[config_name] = config_value;
  }

  RayConfig::instance().initialize(raylet_config);

  boost::asio::io_context ioc;

  auto http_server = std::make_shared<HttpServer>(ioc);
  http_server->Start("0.0.0.0", 8080);

  auto gcs_server = std::make_shared<GcsServer>(ioc);
  gcs_server->Start("0.0.0.0", 8081);

  ioc.run();
}

}

}
