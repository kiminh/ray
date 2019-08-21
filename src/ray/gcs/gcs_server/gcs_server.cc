#include "gcs_server.h"
#include <ray/common/ray_config.h>
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

    // Auto Route Uri
    AutoRouteUri(boost::system::errc::make_error_code(boost::system::errc::success),
                 std::make_shared<boost::asio::deadline_timer>(ioc_));
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

void GcsServer::AutoRouteUri(boost::system::error_code err,
                             std::shared_ptr<boost::asio::deadline_timer> timer) {
  if (has_route_table_) {
    return;
  }

  RAY_LOG(INFO) << "[" << __FUNCTION__ << "] get route table";
  auto iter_alive = [this](std::function<bool(const GcsNodeInfo &)> cb) {
    auto client_infos = gcs_client_->client_table().GetAllClients();
    for (auto &client_info : client_infos) {
      if (client_info.second.state() == GcsNodeInfo::ALIVE) {
        if (cb(client_info.second)) break;
      }
    }
  };

  iter_alive([this](const GcsNodeInfo &node) {
    auto &host = node.node_manager_address();
    auto port = node.raylet_http_port();
    auto http_client = std::make_shared<HttpSyncClient>();
    if (http_client->Connect(host, port)) {
      auto all_uris =
          http_client->Get("/help", std::unordered_map<std::string, std::string>());
      if (all_uris.first.value() == boost::system::errc::errc_t::success) {
        try {
          rapidjson::Document doc;
          doc.Parse(all_uris.second);
          for (auto it = doc.MemberBegin(); it != doc.MemberEnd(); ++it) {
            auto uri = it->name.GetString();
            auto help = it->value.GetString();
            HttpRouter::Register(uri, help,
                                 [this, uri](HttpParams &&params, std::string &&data,
                                             std::shared_ptr<HttpReply> r) {
                                   CollectRayletInfo(uri, std::move(params),
                                                     std::move(data), r);
                                 });
          }
          has_route_table_ = true;
          return true;
        } catch (const std::exception &exc) {
          RAY_LOG(ERROR) << "AutoRouteUri parse /help result as json failed: "
                         << exc.what();
        }
      }
    }
    return false;
  });

  if (!has_route_table_) {
    timer->expires_from_now(boost::posix_time::seconds(5));
    timer->async_wait(boost::bind(&GcsServer::AutoRouteUri, this,
                                  boost::asio::placeholders::error, timer));
  }
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
