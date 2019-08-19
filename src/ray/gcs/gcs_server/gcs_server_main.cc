#include "gcs_server.h"

#include "ray/common/ray_config.h"
#include "ray/util/util.h"

#include "gcs_server.h"
#include "gflags/gflags.h"
#include "ray/failure_detector/failure_detector_slave.h"
#include "ray/http/http_server.h"

#include <boost/asio.hpp>
using tcp = boost::asio::ip::tcp;

DEFINE_string(role, "master", "role of the instance.");
DEFINE_string(logs_dir, "", "log directory.");
DEFINE_string(config_path, "", "log directory.");
DEFINE_int32(gcs_port, 8081, "server port.");
DEFINE_int32(http_port, 8080, "http port.");

int main(int argc, char *argv[]) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  const std::string role = FLAGS_role;
  const std::string logs_dir = FLAGS_logs_dir;
  const std::string config_path = FLAGS_config_path;
  const int gcs_port = static_cast<int>(FLAGS_gcs_port);
  const int http_port = static_cast<int>(FLAGS_http_port);
  gflags::ShutDownCommandLineFlags();

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

    auto endpoint = boost::asio::ip::detail::endpoint(local_address, gcs_port);
    slave_fd->Run(config_path, endpoint, 1000);
    ioc.run();
  } else {
    std::cout << "args error!" << std::endl;
    return 0;
  }

  return 0;
}
