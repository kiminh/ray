#include "gcs_server.h"
#include "failover/l1_failover.h"

namespace ray {

namespace gcs {

GcsServer::GcsServer(boost::asio::io_context &ioc)
    : ioc_(ioc)
    , failover_(new L1Failover(fd_)){
}

void GcsServer::Start(const tcp::endpoint &ep) {

}

void GcsServer::Start(const std::string &host, uint16_t port) {
  Start(tcp::endpoint(boost::asio::ip::make_address(host), port));
}

void GcsServer::OnRegister(const rpc::RegisterRequest &req,
                           rpc::RegisterReply &reply) {
  failover_->TryRegister(req, reply);
}

}

}
