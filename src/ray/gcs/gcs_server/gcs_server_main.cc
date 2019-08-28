#include "gcs_server.h"

#include "ray/common/ray_config.h"
#include "ray/util/util.h"

#include "gcs_server.h"
#include "gflags/gflags.h"
#include "ray/failure_detector/failure_detector_slave.h"
#include "ray/http/http_server.h"
#include "ray/util/json.h"

#include <fstream>

#include <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;

DEFINE_string(role, "master", "role of the instance.");
DEFINE_string(logs_dir, "", "log directory.");
DEFINE_string(json_config_file, "", "log directory.");
DEFINE_int32(gcs_port, 8081, "server port.");
DEFINE_int32(http_port, 8080, "http port.");

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string role = FLAGS_role;
  const std::string logs_dir = FLAGS_logs_dir;
  const std::string json_config_file = FLAGS_json_config_file;
  const int gcs_port = static_cast<int>(FLAGS_gcs_port);
  const int http_port = static_cast<int>(FLAGS_http_port);
  gflags::ShutDownCommandLineFlags();

  {
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

    std::unordered_map<std::string, std::string> raylet_config;
    raylet_config.emplace("check_interval_seconds",
                          std::to_string(check_interval_seconds));
    raylet_config.emplace("beacon_interval_seconds",
                          std::to_string(beacon_interval_seconds));
    raylet_config.emplace("lease_seconds", std::to_string(lease_seconds));
    raylet_config.emplace("grace_seconds", std::to_string(grace_seconds));

    auto redis_passw = doc["redis"]["passw"].GetString();
    auto redis_address = doc["redis"]["address"].GetString();
    auto redis_port = doc["redis"]["port"].GetInt();
    raylet_config.emplace("redis_passw", redis_passw);
    raylet_config.emplace("redis_address", redis_address);
    raylet_config.emplace("redis_port", std::to_string(redis_port));

    RayConfig::instance().initialize(raylet_config);
  }

  InitShutdownRAII ray_log_shutdown_raii(ray::RayLog::StartRayLog,
                                         ray::RayLog::ShutDownRayLog, argv[0],
                                         ray::RayLogLevel::INFO, /*log_dir=*/
                                         logs_dir);
  ray::RayLog::InstallFailureSignalHandler();

  boost::asio::io_context ioc;

  if (role == "master") {
    auto http_server = std::make_shared<ray::HttpServer>(ioc);
    http_server->Start("0.0.0.0", http_port);

    auto gcs_server = std::make_shared<ray::gcs::GcsServer>("GcsServer", gcs_port, ioc);
    gcs_server->StartRpcService();
    ioc.run();
  } else if (role == "slave") {
    auto local_address = get_local_address(ioc);
    uint64_t node_id = local_address.to_v4().to_uint();
    node_id = node_id << 32u;
    // node_id |= uint32_t(getpid());

    auto slave_fd = std::make_shared<ray::fd::FailureDetectorSlave>(ioc);
    slave_fd->SetNodeId(node_id);

    auto primary_endpoint = boost::asio::ip::detail::endpoint(local_address, 0);
    slave_fd->SetPrimaryEndpoint(primary_endpoint);

    slave_fd->Run(boost::asio::ip::detail::endpoint(local_address, gcs_port));
    ioc.run();
  } else {
    std::cout << "args error!" << std::endl;
    return 0;
  }

  return 0;
}
