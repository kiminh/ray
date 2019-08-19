#include "gcs_server.h"
#include "failover/l1_failover.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const std::string &name, uint16_t port, boost::asio::io_context &ioc)
    : ioc_(ioc),
      fd_(std::make_shared<FailureDetectMaster>(ioc_)),
      failover_(new L1Failover(ioc_, fd_)),
      gcs_server_(name, port, ioc_),
      gcs_service_(*this) {
  auto local_address = get_local_address(ioc_);
  auto local_endpoint = boost::asio::ip::detail::endpoint(local_address, port);
  RAY_LOG(DEBUG) << "local endpoint " << local_endpoint.to_string();
  fd_->SetPrimaryEndpoint(local_endpoint);
}

void GcsServer::StartRpcService() {
  fd_->RegiserServiceTo(gcs_server_);
  gcs_server_.RegisterService(gcs_service_);
  gcs_server_.Run();
}

void GcsServer::StopRpcService() { gcs_server_.Shutdown(); }

void GcsServer::HandleRegister(const rpc::RegisterRequest &req, rpc::RegisterReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  auto status = failover_->TryRegister(req, reply);
  send_reply_callback(status, nullptr, nullptr);
}

}  // namespace gcs
}  // namespace ray
