#include "gcs_server.h"
#include "failover/l1_failover.h"
#include "ray/common/ray_config.h"
#include "ray/util/json.h"

namespace ray {
namespace gcs {

GcsServer::GcsServer(const std::string &name, uint16_t port, boost::asio::io_context &ioc)
    : ioc_(ioc),
      fd_(ioc_),
      failover_(new L1Failover(ioc_, fd_)),
      gcs_server_(name, port, ioc_),
      gcs_service_(*this) {
  auto local_address = get_local_address(ioc_);
  auto local_endpoint = boost::asio::ip::detail::endpoint(local_address, port);
  RAY_LOG(DEBUG) << "local endpoint " << local_endpoint.to_string();
  fd_.SetPrimaryEndpoint(local_endpoint);

  auto redis_port = RayConfig::instance().redis_port();
  auto redis_address = RayConfig::instance().redis_address();
  auto redis_passw = RayConfig::instance().redis_passw();
  GcsClientOptions options(redis_address, redis_port, redis_passw);
  gcs_client_ = std::make_shared<RedisGcsClient>(options);
}

void GcsServer::StartRpcService() {
  fd_.RegiserServiceTo(gcs_server_);
  gcs_server_.RegisterService(gcs_service_);
  gcs_server_.Run();
  AutoRouteUri(boost::system::errc::make_error_code(boost::system::errc::success),
               std::make_shared<boost::asio::deadline_timer>(ioc_));
}

void GcsServer::StopRpcService() { gcs_server_.Shutdown(); }

void GcsServer::HandleRegister(const rpc::RegisterRequest &req, rpc::RegisterReply *reply,
                               rpc::SendReplyCallback send_reply_callback) {
  auto status = failover_->TryRegister(req, reply);
  send_reply_callback(status, nullptr, nullptr);
}

void GcsServer::AutoRouteUri(boost::system::error_code err,
                             std::shared_ptr<boost::asio::deadline_timer> timer) {
  if (has_route_table_) {
    return;
  }
  RAY_LOG(INFO) << "AutoRouteUri get route table";
  auto client_infos = gcs_client_->client_table().GetAllClients();
  if (!client_infos.empty()) {
    auto host = client_infos.begin()->second.node_manager_address();
    auto port = client_infos.begin()->second.raylet_http_port();
    auto http_client = std::make_shared<HttpSyncClient>();
    http_client->Connect(host, port);
    auto all_uris =
        http_client->Get("/help", std::unordered_map<std::string, std::string>());
    try {
      rapidjson::Document doc;
      doc.Parse(all_uris);
      for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
        auto uri = it->name.GetString();
        auto help = it->value.GetString();
        HttpRouter::Register(uri, help,
                             [this, uri](HttpParams &&params, std::string &&data,
                                         std::shared_ptr<HttpReply> r) {
                               CollectRayletInfo(uri, std::move(params), std::move(data),
                                                 r);
                             });
      }
      has_route_table_ = true;
      return;
    } catch (const std::exception &exc) {
      RAY_LOG(ERROR) << "AutoRouteUri parse /help result as json failed: " << exc.what();
    }
  }

  timer->expires_from_now(boost::posix_time::seconds(5));
  timer->async_wait(boost::bind(&GcsServer::AutoRouteUri, this,
                                boost::asio::placeholders::error, timer));
}

void GcsServer::CollectRayletInfo(const std::string &uri, HttpParams &&params,
                                  std::string &&data, std::shared_ptr<HttpReply> r) {
  auto client_infos = gcs_client_->client_table().GetAllClients();
  auto doc = std::make_shared<rapidjson::Document>(rapidjson::kObjectType);
  auto counter = std::make_shared<int>(client_infos.size());
  for (auto &i : client_infos) {
    auto host = i.second.node_manager_address();
    auto port = i.second.raylet_http_port();
    auto it = http_clients_.find(i.first);
    if (it == http_clients_.end() || !it->second->IsConnected()) {
      auto http_client = std::make_shared<HttpAsyncClient>(ioc_);
      http_client->Connect(host, port);
      if (it == http_clients_.end()) {
        it = http_clients_.emplace(i.first, http_client).first;
      } else {
        http_clients_[i.first] = http_client;
      }
    }
    auto cid = i.first.Hex();
    it->second->Get(
        uri, params,
        [cid, doc, r, counter](boost::system::error_code ec, const std::string &s) {
          rapidjson::Document::AllocatorType &alloc = doc->GetAllocator();
          try {
            rapidjson::Document doc_r(&alloc);
            doc_r.Parse(s);
            doc->AddMember(rapidjson::Value(cid.c_str(), alloc), doc_r, alloc);
          } catch (const std::exception &exc) {
            doc->AddMember(rapidjson::Value(cid.c_str(), alloc),
                           rapidjson::Value(s, alloc), alloc);
          }
          if (--(*counter) == 0) {
            r->SetJsonContent(rapidjson::to_string(*doc, true));
          }
        });
  }
}

}  // namespace gcs
}  // namespace ray
