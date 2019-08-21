#include "raylet.h"

#include <ray/http/http_server.h>
#include <ray/raylet/failover/l1_failover.h>
#include <boost/asio.hpp>
#include <boost/bind.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <iostream>

#include "ray/common/status.h"

namespace {

const std::vector<std::string> GenerateFlatbufEnumNames(const char *const *enum_names_ptr,
                                                        int start_index, int end_index) {
  std::vector<std::string> enum_names;
  for (int i = 0; i < start_index; ++i) {
    enum_names.push_back("EmptyMessageType");
  }
  size_t i = 0;
  while (true) {
    const char *name = enum_names_ptr[i];
    if (name == nullptr) {
      break;
    }
    enum_names.push_back(name);
    i++;
  }
  RAY_CHECK(static_cast<size_t>(end_index) == enum_names.size() - 1)
      << "Message Type mismatch!";
  return enum_names;
}

static const std::vector<std::string> node_manager_message_enum =
    GenerateFlatbufEnumNames(ray::protocol::EnumNamesMessageType(),
                             static_cast<int>(ray::protocol::MessageType::MIN),
                             static_cast<int>(ray::protocol::MessageType::MAX));
}  // namespace

namespace ray {

namespace raylet {

Raylet::Raylet(boost::asio::io_service &main_service, const std::string &socket_name,
               const std::string &node_ip_address, const std::string &redis_address,
               int redis_port, const std::string &redis_password,
               const NodeManagerConfig &node_manager_config,
               const ObjectManagerConfig &object_manager_config,
               std::shared_ptr<gcs::RedisGcsClient> gcs_client)
    : fd_(std::make_shared<fd::FailureDetectorSlave>(fd_service_)),
      failover_(std::make_shared<L1Failover>(fd_)),
      gcs_client_(gcs_client),
      object_directory_(std::make_shared<ObjectDirectory>(main_service, gcs_client_)),
      object_manager_(main_service, object_manager_config, object_directory_),
      node_manager_(main_service, node_manager_config, object_manager_, gcs_client_,
                    object_directory_, failover_),
      socket_name_(socket_name),
      acceptor_(main_service, boost::asio::local::stream_protocol::endpoint(socket_name)),
      socket_(main_service) {
  if (RayConfig::instance().enable_l1_failover()) {
    InitFd(node_ip_address);
    RegisterGcsServer([this, node_ip_address, object_manager_config, redis_address,
                       redis_port, redis_password, node_manager_config] {
      Start(acceptor_.get_io_context(), node_ip_address, socket_name_,
            object_manager_config.store_socket_name, redis_address, redis_port,
            redis_password, node_manager_config);
    });
  } else {
    Start(main_service, node_ip_address, socket_name_,
          object_manager_config.store_socket_name, redis_address, redis_port,
          redis_password, node_manager_config);
  }
}

Raylet::~Raylet() {
  fd_->Stop();
  fd_service_.stop();
  if (fd_thread_) {
    fd_thread_->join();
  }
}

void Raylet::InitFd(const std::string &node_ip_address) {
  auto local_address = boost::asio::ip::make_address_v4(node_ip_address);
  auto port = node_manager_.GetServerPort();
  boost::asio::ip::detail::endpoint primary_endpoint(local_address, port);

  uint64_t node_id = endpoint_to_uint64(primary_endpoint);
  auto ppid = getppid();
  if (ppid != 1) {
    node_id = (uint64_t(local_address.to_uint()) << 32u) | uint64_t(ppid);
  }

  ip::detail::endpoint gcs_server_endpoint;
  for (;;) {
    auto redis_context = gcs_client_->primary_context();
    auto reply = redis_context->RunArgvSync({"GET", kGcsServerAddress});
    if (!reply || reply->IsNil()) {
      // Maybe the gcs server has not registered to gcs yet.
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }
    auto gcs_host_port = reply->ReadAsString();
    gcs_server_endpoint = endpoint_from_string(gcs_host_port);
    break;
  }

  std::promise<bool> connected_promise;
  auto connected_future = connected_promise.get_future();
  fd_->OnMasterConnected([&connected_promise](const ip::detail::endpoint &endpoint) {
    connected_promise.set_value(true);
  });
  fd_->SetPrimaryEndpoint(primary_endpoint);
  fd_->SetNodeId(node_id);
  fd_->Run(gcs_server_endpoint);
  fd_thread_.reset(new std::thread([this] { fd_service_.run(); }));
  auto lease_ms = fd_->GetLeaseMillisecond();
  auto status = connected_future.wait_for(std::chrono::milliseconds(lease_ms));
  RAY_CHECK(status == std::future_status::ready) << "[" << __FUNCTION__ << "] failed!";
}

uint16_t Raylet::StartHttpServer(boost::asio::io_service &ioc) {
  // Initialize http server
  auto http_server = std::make_shared<ray::HttpServer>(ioc);
  http_server->Start("0.0.0.0", 0);
  return http_server->Port();
}

void Raylet::RegisterGcsServer(std::function<void()> &&callback) {
  auto &main_service = acceptor_.get_io_context();

  auto node_manager_port = node_manager_.GetServerPort();
  auto local_address = get_local_address(main_service);
  boost::asio::ip::detail::endpoint primary_endpoint(local_address, node_manager_port);

  rpc::RegisterRequest request;
  request.set_secret(failover_->GetSecret());
  request.set_address(endpoint_to_uint64(primary_endpoint));
  request.set_node_id(fd_->GetNodeId());

  auto gcs_server_endpoint = fd_->GetMasterEndpoint();
  auto gcs_server_address = gcs_server_endpoint.address().to_string();
  auto gcs_server_port = gcs_server_endpoint.port();
  auto gcs_asio_client = std::make_shared<rpc::GcsAsioClient>(
      gcs_server_address, gcs_server_port, main_service);
  RegisterGcsServerWithRetry(gcs_asio_client, request, callback);
}

void Raylet::RegisterGcsServerWithRetry(const std::shared_ptr<rpc::GcsAsioClient> &client,
                                        const rpc::RegisterRequest &req,
                                        const std::function<void()> &fn) {
  auto callback = [=](const Status &status, const rpc::RegisterReply &reply) {
    if (status.ok() && reply.success()) {
      if (reply.success()) {
        if (fn) {
          fn();
        }
      } else {
        execute_after(acceptor_.get_io_context(),
                      [=] { RegisterGcsServerWithRetry(client, req, fn); }, 1000);
      }
    } else {
      RAY_LOG(FATAL) << "GCS Server " << fd_->GetMasterEndpoint().to_string()
                     << " may be dead, just exit!";
    }
  };

  auto status = client->Register(req, callback);
  RAY_CHECK(status.ok()) << "GCS Server " << fd_->GetMasterEndpoint().to_string()
                         << " may be dead, just exit!";
}

void Raylet::Start(boost::asio::io_service &main_service,
                   const std::string &node_ip_address,
                   const std::string &raylet_socket_name,
                   const std::string &object_store_socket_name,
                   const std::string &redis_address, int redis_port,
                   const std::string &redis_password,
                   const ray::raylet::NodeManagerConfig &node_manager_config) {
  // Start http server
  auto http_port = StartHttpServer(main_service);

  // Start listening for clients.
  DoAccept();

  RAY_CHECK_OK(RegisterGcs(node_ip_address, raylet_socket_name, object_store_socket_name,
                           redis_address, redis_port, redis_password, http_port,
                           main_service, node_manager_config));

  RAY_CHECK_OK(RegisterPeriodicTimer(main_service));
}

ray::Status Raylet::RegisterPeriodicTimer(boost::asio::io_service &io_service) {
  boost::posix_time::milliseconds timer_period_ms(100);
  boost::asio::deadline_timer timer(io_service, timer_period_ms);
  return ray::Status::OK();
}

ray::Status Raylet::RegisterGcs(const std::string &node_ip_address,
                                const std::string &raylet_socket_name,
                                const std::string &object_store_socket_name,
                                const std::string &redis_address, int redis_port,
                                const std::string &redis_password, int http_port,
                                boost::asio::io_service &io_service,
                                const NodeManagerConfig &node_manager_config) {
  GcsNodeInfo node_info = gcs_client_->client_table().GetLocalClient();
  node_info.set_node_manager_address(node_ip_address);
  node_info.set_raylet_socket_name(raylet_socket_name);
  node_info.set_object_store_socket_name(object_store_socket_name);
  node_info.set_object_manager_port(object_manager_.GetServerPort());
  node_info.set_node_manager_port(node_manager_.GetServerPort());
  node_info.set_raylet_http_port(http_port);

  RAY_LOG(DEBUG) << "Node manager " << gcs_client_->client_table().GetLocalClientId()
                 << " started on " << node_info.node_manager_address() << ":"
                 << node_info.node_manager_port() << " object manager at "
                 << node_info.node_manager_address() << ":"
                 << node_info.object_manager_port() << " http server listen at "
                 << node_info.node_manager_address() << ":" << http_port;
  ;
  RAY_RETURN_NOT_OK(gcs_client_->client_table().Connect(node_info));

  // Add resource information.
  std::unordered_map<std::string, std::shared_ptr<gcs::ResourceTableData>> resources;
  for (const auto &resource_pair : node_manager_config.resource_config.GetResourceMap()) {
    auto resource = std::make_shared<gcs::ResourceTableData>();
    resource->set_resource_capacity(resource_pair.second);
    resources.emplace(resource_pair.first, resource);
  }
  RAY_RETURN_NOT_OK(gcs_client_->resource_table().Update(
      JobID::Nil(), gcs_client_->client_table().GetLocalClientId(), resources, nullptr));

  RAY_RETURN_NOT_OK(node_manager_.RegisterGcs());

  return Status::OK();
}

void Raylet::DoAccept() {
  acceptor_.async_accept(socket_, boost::bind(&Raylet::HandleAccept, this,
                                              boost::asio::placeholders::error));
}

void Raylet::HandleAccept(const boost::system::error_code &error) {
  if (!error) {
    // TODO: typedef these handlers.
    ClientHandler<boost::asio::local::stream_protocol> client_handler =
        [this](LocalClientConnection &client) { node_manager_.ProcessNewClient(client); };
    MessageHandler<boost::asio::local::stream_protocol> message_handler =
        [this](std::shared_ptr<LocalClientConnection> client, int64_t message_type,
               uint64_t length, const uint8_t *message) {
          node_manager_.ProcessClientMessage(client, message_type, message);
        };
    // Accept a new local client and dispatch it to the node manager.
    auto new_connection = LocalClientConnection::Create(
        client_handler, message_handler, std::move(socket_), "worker",
        node_manager_message_enum,
        static_cast<int64_t>(protocol::MessageType::DisconnectClient));
  }
  // We're ready to accept another client.
  DoAccept();
}

}  // namespace raylet

}  // namespace ray
