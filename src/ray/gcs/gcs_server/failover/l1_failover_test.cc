#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/ray_config.h"

#include <ray/failure_detector/failure_detector_master.h>
#include <ray/failure_detector/failure_detector_slave.h>
#include <ray/gcs/gcs_server/failover/failover.h>
#include <ray/gcs/gcs_server/failover/l1_failover.h>
#include <ray/gcs/gcs_server/gcs_server.h>
#include <ray/raylet/failover/l1_failover.h>
#include <ray/rpc/failover/failover_server.h>
#include <ray/rpc/gcs/gcs_client.h>
#include <ray/rpc/gcs/gcs_server.h>
#include <string>
#include <thread>
#include <vector>

namespace ray {

class MockedL1Failover : public raylet::L1Failover {
 public:
  explicit MockedL1Failover(uint64_t node_id,
                            const std::shared_ptr<fd::FailureDetector> &fd)
      : raylet::L1Failover(fd), node_id_(node_id) {}

  void OnExit(std::function<void(int)> &&fn) { on_exit_ = std::move(fn); }

 protected:
  void DoExit(int code) override {
    if (on_exit_) {
      RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id " << node_id_ << " code "
                       << code;
      on_exit_(code);
    }
  }

 private:
  uint64_t node_id_ = 0;
  std::function<void(int)> on_exit_;
};

class MockedNode {
 public:
  explicit MockedNode(uint64_t node_id, uint16_t gcs_server_port)
      : node_id_(node_id), gcs_server_port_(gcs_server_port) {
    Init();
  }

  ~MockedNode() { Stop(); }

  bool Start(uint16_t node_manager_port) {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id_;
    assert(thread_ == nullptr);
    Init();

    auto node_manager_asio_server =
        std::make_shared<rpc::AsioRpcServer>("NodeManager", node_manager_port, *ioc_);
    failover_->RegisterServiceTo(*node_manager_asio_server);
    node_manager_asio_server->Run();
    node_manager_port_ = node_manager_asio_server->GetPort();
    thread_.reset(new std::thread([this, node_manager_asio_server] { ioc_->run(); }));

    if (!InitFd()) {
      return false;
    }

    ioc_->post([this] { Register(node_id_); });
    return true;
  }

  void OnRegistered(std::function<void()> &&fn) { register_listener_ = std::move(fn); }

  void OnExit(std::function<void(int)> &&fn) { on_exit_ = std::move(fn); }

  void Stop() {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id_;
    if (thread_) {
      ioc_->stop();
      fd_->Stop();
      thread_->join();
    }

    ioc_.reset();
    fd_.reset();
    failover_.reset();
    thread_.reset();
  }

  void Restart() {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id_;
    Stop();
    Start(node_manager_port_);
  }

  fd::FailureDetectorSlave *GetFailureDetector() { return fd_.get(); }

 private:
  void Init() {
    if (ioc_ == nullptr) {
      ioc_.reset(new boost::asio::io_context);
    }

    if (fd_ == nullptr) {
      fd_ = std::make_shared<fd::FailureDetectorSlave>(*ioc_);
      fd_->SetNodeId(node_id_);
    }

    if (failover_ == nullptr) {
      failover_ = std::make_shared<MockedL1Failover>(node_id_, fd_);
      failover_->OnExit([this](int code) {
        if (on_exit_) {
          on_exit_(code);
        }
      });
    }
  }

  bool InitFd() {
    auto local_address = get_local_address(*ioc_);
    fd_->SetPrimaryEndpoint(ip::detail::endpoint(local_address, node_manager_port_));

    ip::detail::endpoint gcs_server_endpoint(local_address, gcs_server_port_);
    fd_->Run(gcs_server_endpoint);

    std::promise<bool> connected_promise;
    auto conntected_future = connected_promise.get_future();
    fd_->OnMasterConnected([&connected_promise](const ip::detail::endpoint &endpoint) {
      connected_promise.set_value(true);
    });

    auto status =
        conntected_future.wait_for(std::chrono::milliseconds(fd_->GetLeaseMillisecond()));
    return status == std::future_status::ready;
  }

  void RegisterGcsServerWithRetry(std::shared_ptr<rpc::GcsAsioClient> client,
                                  const rpc::RegisterRequest &req) {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << req.node_id();
    auto callback = [=](const Status &status, const rpc::RegisterReply &reply) {
      if (status.ok()) {
        if (reply.success()) {
          if (register_listener_) {
            register_listener_();
          }
        } else {
          execute_after(*ioc_, [=] { RegisterGcsServerWithRetry(client, req); }, 100);
        }
      } else {
        if (on_exit_) {
          on_exit_(kExitMasterDisconnected);
        }
      }
    };

    auto status = client->Register(req, callback);
    if (status.IsInvalid()) {
      if (on_exit_) {
        on_exit_(kExitMasterDisconnected);
      }
    }
  }

  void Register(uint64_t node_id) {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id;
    assert(ioc_);
    auto local_address = get_local_address(*ioc_);
    ip::detail::endpoint primary_endpoint(local_address, node_manager_port_);

    rpc::RegisterRequest request;
    request.set_secret(failover_->GetSecret());
    request.set_address(endpoint_to_uint64(primary_endpoint));
    request.set_node_id(node_id);

    auto gcs_server_endpoint = fd_->GetMasterEndpoint();
    auto gcs_server_address = gcs_server_endpoint.address().to_string();
    auto gcs_server_port = gcs_server_endpoint.port();
    auto gcs_asio_client =
        std::make_shared<rpc::GcsAsioClient>(gcs_server_address, gcs_server_port, *ioc_);
    RegisterGcsServerWithRetry(gcs_asio_client, request);
  }

 private:
  uint64_t node_id_ = 0;
  uint16_t gcs_server_port_ = 0;
  uint16_t node_manager_port_ = 0;
  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<boost::asio::io_context> ioc_;
  std::shared_ptr<fd::FailureDetectorSlave> fd_;
  std::shared_ptr<MockedL1Failover> failover_;
  std::function<void()> register_listener_;
  std::function<void(int)> on_exit_;
};

class MockedGcsServer {
  class GcsServer : public ray::gcs::GcsServer {
   public:
    explicit GcsServer(const std::string &name, uint16_t port,
                       boost::asio::io_context &ioc)
        : ray::gcs::GcsServer(name, port, ioc) {}

    void Start() override {
      // Run Server
      StartServer();

      // Run fd
      StartFd();
    }

    void StartMonitor() override {
      // Ignore
    }
  };

 public:
  ~MockedGcsServer() { Stop(); }

  uint16_t Start(const std::string &name, uint16_t port) {
    assert(thread_ == nullptr);
    Init();

    gcs_server_.reset(new GcsServer(name, port, *ioc_));
    gcs_server_->Start();
    auto gcs_server_port = gcs_server_->GetPort();

    thread_.reset(new std::thread([this] { ioc_->run(); }));
    return gcs_server_port;
  }

  void Stop() {
    if (thread_) {
      gcs_server_->Stop();
      ioc_->stop();
      thread_->join();
    }

    ioc_.reset();
    gcs_server_.reset();
    thread_.reset();
  }

 private:
  void Init() {
    if (ioc_ == nullptr) {
      ioc_.reset(new boost::asio::io_context);
    }
  }

 private:
  std::unique_ptr<std::thread> thread_;
  std::unique_ptr<boost::asio::io_context> ioc_;
  std::unique_ptr<GcsServer> gcs_server_;
};

class L1FailoverTest : public ::testing::Test {
 public:
  L1FailoverTest() {
    if (!RayConfig::instance().initialized_) {
      RayConfig::instance().initialize({{"check_interval_seconds", "2"},
                                        {"beacon_interval_seconds", "2"},
                                        {"lease_seconds", "4"},
                                        {"grace_seconds", "5"}});
    }
  }
};

TEST_F(L1FailoverTest, NodeRegister) {
  MockedGcsServer server;
  uint16_t gcs_server_port = server.Start("GcsServer", 0);

  int node_count = 5;
  std::vector<std::promise<bool>> registered_promises(node_count);
  std::vector<std::future<bool>> registered_futures(node_count);
  std::vector<std::unique_ptr<MockedNode>> nodes(node_count);
  for (int i = 0; i < node_count; ++i) {
    registered_futures[i] = registered_promises[i].get_future();
    nodes[i].reset(new MockedNode(i + 1, gcs_server_port));
    nodes[i]->OnRegistered(
        [i, &registered_promises] { registered_promises[i].set_value(true); });
    ASSERT_TRUE(nodes[i]->Start(0));
  }

  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(std::future_status::ready,
              registered_futures[i].wait_for(std::chrono::milliseconds(3000)));
    ASSERT_EQ(true, registered_futures[i].get());
  }
}

TEST_F(L1FailoverTest, RestartNodeOneByOne) {
  std::vector<std::thread> threads;
  MockedGcsServer server;
  uint16_t gcs_server_port = server.Start("GcsServer", 0);

  int round = 1;
  int node_count = 5;
  std::vector<std::promise<int>> registered_promises(node_count);
  std::vector<std::future<int>> registered_futures(node_count);
  std::vector<std::unique_ptr<MockedNode>> nodes(node_count);
  for (int i = 0; i < node_count; ++i) {
    registered_futures[i] = registered_promises[i].get_future();
    nodes[i].reset(new MockedNode(i + 1, gcs_server_port));
    auto node = nodes[i].get();
    node->OnExit(
        [node, &threads](int) { threads.emplace_back([node] { node->Restart(); }); });
    node->OnRegistered(
        [i, &round, &registered_promises] { registered_promises[i].set_value(round); });
    ASSERT_TRUE(nodes[i]->Start(0));
  }

  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(std::future_status::ready,
              registered_futures[i].wait_for(std::chrono::milliseconds(3000)));
    ASSERT_EQ(round, registered_futures[i].get());
  }

  for (int k = 0; k < node_count; ++k) {
    ++round;
    for (int i = 0; i < node_count; ++i) {
      registered_promises[i] = std::promise<int>();
      registered_futures[i] = registered_promises[i].get_future();
    }
    RAY_LOG(INFO) << "Restart node " << k
                  << " and check if all other nodes restarted and register again.";
    threads.emplace_back([k, &nodes] { nodes[k]->Restart(); });
    for (int i = 0; i < node_count; ++i) {
      ASSERT_EQ(std::future_status::ready,
                registered_futures[i].wait_for(std::chrono::milliseconds(10000)));
      ASSERT_EQ(round, registered_futures[i].get());
    }
  }

  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(L1FailoverTest, StopAllNodesAndRestartAgain) {
  std::vector<std::thread> threads;
  MockedGcsServer server;
  uint16_t gcs_server_port = server.Start("GcsServer", 0);

  int round = 1;
  int node_count = 5;
  std::vector<std::promise<int>> registered_promises(node_count);
  std::vector<std::future<int>> registered_futures(node_count);
  std::vector<std::unique_ptr<MockedNode>> nodes(node_count);
  for (int i = 0; i < node_count; ++i) {
    registered_futures[i] = registered_promises[i].get_future();
    nodes[i].reset(new MockedNode(i + 1, gcs_server_port));
    auto node = nodes[i].get();
    node->OnExit(
        [node, &threads](int) { threads.emplace_back([node] { node->Restart(); }); });
    node->OnRegistered(
        [i, &round, &registered_promises] { registered_promises[i].set_value(round); });
    ASSERT_TRUE(nodes[i]->Start(0));
  }

  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(std::future_status::ready,
              registered_futures[i].wait_for(std::chrono::milliseconds(3000)));
    ASSERT_EQ(round, registered_futures[i].get());
  }

  ++round;
  for (int i = 0; i < node_count; ++i) {
    registered_promises[i] = std::promise<int>();
    registered_futures[i] = registered_promises[i].get_future();
  }

  RAY_LOG(INFO) << "Stop all nodes.";
  for (int i = 0; i < node_count; ++i) {
    auto node = nodes[i].get();
    node->OnExit(nullptr);
    node->Stop();
  }

  RAY_LOG(INFO) << "Restart all nodes "
                   " and check if all other nodes restarted and register again.";
  for (int i = 0; i < node_count; ++i) {
    auto node = nodes[i].get();
    node->OnExit(
        [node, &threads](int) { threads.emplace_back([node] { node->Restart(); }); });
    threads.emplace_back([node] { node->Restart(); });
  }

  for (int i = 0; i < node_count; ++i) {
    ASSERT_EQ(std::future_status::ready,
              registered_futures[i].wait_for(std::chrono::milliseconds(10000)));
    ASSERT_EQ(round, registered_futures[i].get());
  }

  for (auto &t : threads) {
    t.join();
  }
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
