#include "gcs_server.h"
#include <ray/common/ray_config.h>
#include "failover/l1_failover.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const std::string &name, uint16_t port, boost::asio::io_context &ioc)
    : ioc_(ioc),
      fd_(ioc_),
      failover_(new L1Failover(ioc_, fd_)),
      gcs_server_(name, port, ioc_),
      gcs_service_(*this) {
  failover_->OnRoundFailedBegin([this] { StopMonitor(); });
  failover_->OnRoundFailedEnd([this] {
    ClearnGcs();
    StartMonitor();
  });
}

void GcsServer::Start() {
  if (RayConfig::instance().enable_l1_failover()) {
    // init gcs client
    InitGcsClient();

    InitGcsGCManager();

    if (IsGcsServerAddressExist()) {
      // The gcs is restarted so we need to pause fd until all raylet restarted.
      // Use sleep to make sure all raylet is restared for the time being.
      // TODO(hc): optimize it later
      std::this_thread::sleep_for(std::chrono::milliseconds(fd_.GetGraceMillisecond()));
    }

    // Start monitor
    StartMonitor();

    // Start Server
    StartServer();

    // Start fd
    StartFd();

    // Start Http server
    StartHttpServer();

    // Update gcs server address to gcs
    UpdateGcsServerAddress();
  } else {
    StartMonitor();
  }
}

void GcsServer::Stop() {
  gcs_server_.Shutdown();
  fd_.Stop();
}

void GcsServer::HandleRegister(const rpc::RegisterRequest &req, rpc::RegisterReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  if (!failover_->TryRegister(req, reply)) {
    // just ignore
    RAY_LOG(DEBUG) << "[" << __FUNCTION__ << "] failed! nid: " << req.node_id()
                   << ", just ignore.";
  }
  send_reply_callback(ray::Status::OK(), nullptr, nullptr);
}

void GcsServer::StartMonitor() {
  auto redis_port = RayConfig::instance().redis_port();
  auto redis_address = RayConfig::instance().redis_address();
  auto redis_passw = RayConfig::instance().redis_password();
  monitor_.reset(new ray::raylet::Monitor(ioc_, redis_address, redis_port, redis_passw));
}

void GcsServer::InitGcsClient() {
  auto redis_port = RayConfig::instance().redis_port();
  auto redis_address = RayConfig::instance().redis_address();
  auto redis_passw = RayConfig::instance().redis_password();
  GcsClientOptions options(redis_address, redis_port, redis_passw);
  gcs_client_ = std::make_shared<RedisGcsClient>(options);
  auto status = gcs_client_->Connect(ioc_);
  RAY_CHECK(status.ok()) << "[" << __FUNCTION__ << "] failed as " << status;
}

void GcsServer::InitGcsGCManager() {
  gcs_gc_manager_ = std::make_shared<GcsGCManager>(*gcs_client_);
}

void GcsServer::UpdateGcsServerAddress() {
  auto primary_endpoint = fd_.GetPrimaryEndpoint();
  auto redis_context = gcs_client_->primary_context();
  std::string host_port = primary_endpoint.to_string();
  auto status = redis_context->RunArgvAsync({"SET", kGcsServerAddress, host_port});
  RAY_CHECK(status.ok()) << "[" << __FUNCTION__ << "] "
                         << "update gcs server address to gcs failed! "
                         << "error: " << status;

  auto http_port = http_server_ ? http_server_->Port() : 0;
  auto http_host_port =
      primary_endpoint.address().to_string() + ":" + std::to_string(http_port);
  status = redis_context->RunArgvAsync({"SET", kGcsHttpServerAddress, http_host_port});
  RAY_CHECK(status.ok()) << "[" << __FUNCTION__ << "] "
                         << "update gcs http server address to gcs failed! "
                         << "error: " << status;
}

bool GcsServer::IsGcsServerAddressExist() {
  auto redis_context = gcs_client_->primary_context();
  auto reply = redis_context->RunArgvSync({"GET", kGcsServerAddress});
  return reply != nullptr && !reply->IsNil();
}

void GcsServer::StopMonitor() { monitor_.reset(); }

void GcsServer::StartServer() {
  // Reigster service and start server
  fd_.RegiserServiceTo(gcs_server_);
  gcs_server_.RegisterService(gcs_service_);
  gcs_server_.Run();
}

void GcsServer::StartFd() {
  auto port = gcs_server_.GetPort();
  auto local_address = get_local_address(ioc_);
  auto local_endpoint = boost::asio::ip::detail::endpoint(local_address, port);
  fd_.SetPrimaryEndpoint(local_endpoint);
  fd_.Run();
}

void GcsServer::StartHttpServer() {
  http_server_ = std::make_shared<ray::HttpServer>(ioc_);
  http_server_->Start("0.0.0.0", 0);
  RAY_LOG(INFO) << "[" << __FUNCTION__ << "] http_port: " << http_server_->Port();
}

void GcsServer::ClearnGcs() {
  if (gcs_gc_manager_) {
    auto status = gcs_gc_manager_->CleanForLevelOneFailover();
    RAY_LOG(FATAL) << "[" << __FUNCTION__ << "] failed! error: " << status;
  }
}

}  // namespace gcs
}  // namespace ray
