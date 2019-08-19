#include "failure_detector_master.h"
#include "failure_detector_slave.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "ray/common/ray_config.h"

#include <future>
#include <string>
#include <thread>

namespace ray {
class FailureDetectorTest : public ::testing::Test {
 public:
  FailureDetectorTest() {
    if (!RayConfig::instance().initialized_) {
      RayConfig::instance().initialize({{"check_interval_seconds", "2"},
                                        {"beacon_interval_seconds", "2"},
                                        {"lease_seconds", "4"},
                                        {"grace_seconds", "5"}});
    }
  }

 protected:
  class Master {
   public:
    Master() { Init(); }

    ~Master() { Stop(); }

    int Start(uint16_t port) {
      assert(thread_ == nullptr);
      Init();

      auto server = std::make_shared<rpc::AsioRpcServer>("FailureDetector", port, *ioc_);
      server->Run();
      auto server_port = server->GetPort();

      fd_->SetPrimaryEndpoint(GetEndpoint(server_port));
      fd_->RegiserServiceTo(*server);
      fd_->Run();

      thread_.reset(new std::thread([this, server] { ioc_->run(); }));
      return server_port;
    }

    void Stop() {
      if (thread_) {
        fd_->Stop();
        ioc_->stop();
        thread_->join();
      }

      ioc_.reset();
      fd_.reset();
      thread_.reset();
    }

    void Restart(uint16_t port) {
      Stop();
      Start(port);
    }

    fd::FailureDetectorMaster *GetFailureDetector() { return fd_.get(); }

    ip::detail::endpoint GetEndpoint(int port) {
      auto address = get_local_address(*ioc_);
      return boost::asio::ip::detail::endpoint(address, port);
    }

   private:
    void Init() {
      if (ioc_ == nullptr) {
        ioc_.reset(new boost::asio::io_context);
      }

      if (fd_ == nullptr) {
        fd_.reset(new fd::FailureDetectorMaster(*ioc_));
      }
    }

   private:
    std::unique_ptr<std::thread> thread_;
    std::unique_ptr<boost::asio::io_context> ioc_;
    std::unique_ptr<fd::FailureDetectorMaster> fd_;
  };

  class Slave {
   public:
    explicit Slave(uint64_t node_id, uint16_t server_port)
        : node_id_(node_id), server_port_(server_port) {
      Init();
    }

    ~Slave() { Stop(); }

    void Start() {
      assert(thread_ == nullptr);
      Init();
      fd_->Run(boost::asio::ip::detail::endpoint(get_local_address(*ioc_), server_port_));
      thread_.reset(new std::thread([this] { ioc_->run(); }));
    }

    void Stop() {
      if (thread_) {
        fd_->Stop();
        ioc_->stop();
        thread_->join();
      }

      ioc_.reset();
      fd_.reset();
      thread_.reset();
    }

    void Restart() {
      Stop();
      Start();
    }

    fd::FailureDetectorSlave *GetFailureDetector() { return fd_.get(); }

    ip::detail::endpoint GetEndpoint() {
      auto address = get_local_address(*ioc_);
      return boost::asio::ip::detail::endpoint(address, 0);
    }

   private:
    void Init() {
      if (ioc_ == nullptr) {
        ioc_.reset(new boost::asio::io_context);
      }

      if (fd_ == nullptr) {
        auto address = get_local_address(*ioc_);
        fd_.reset(new fd::FailureDetectorSlave(*ioc_));
        fd_->SetPrimaryEndpoint(boost::asio::ip::detail::endpoint(address, 0));
        fd_->SetNodeId(node_id_);
      }
    }

   private:
    std::unique_ptr<std::thread> thread_;
    uint64_t node_id_ = 0;
    uint16_t server_port_ = 0;
    std::unique_ptr<boost::asio::io_context> ioc_;
    std::unique_ptr<fd::FailureDetectorSlave> fd_;
  };
};

TEST_F(FailureDetectorTest, WorkerConnected) {
  uint64_t node_id = 10010;

  std::promise<fd::WorkerContext> worker_connected_promise;

  auto master = std::make_shared<Master>();
  master->GetFailureDetector()->OnWorkerConnected([&](fd::WorkerContext &&ctx) {
    worker_connected_promise.set_value(std::move(ctx));
  });
  auto server_port = master->Start(0);
  RAY_LOG(INFO) << "Master::Started ...";

  auto slave = std::make_shared<Slave>(node_id, server_port);
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  RAY_LOG(INFO) << "Waiting for [worker_connected] ...";
  auto worker_connected_future = worker_connected_promise.get_future();
  auto status = worker_connected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);

  auto worker_context = worker_connected_future.get();
  ASSERT_EQ(node_id, worker_context.node_id);
}

TEST_F(FailureDetectorTest, WorkerRestarted) {
  uint64_t node_id = 10010;

  std::promise<fd::WorkerContext> worker_connected_promise;
  std::promise<fd::WorkerContext> worker_reconnected_promise;

  auto master = std::make_shared<Master>();
  master->GetFailureDetector()->OnWorkerConnected([&](fd::WorkerContext &&ctx) {
    worker_connected_promise.set_value(std::move(ctx));
  });
  master->GetFailureDetector()->OnWorkerRestartedWithinLease(
      [&](fd::WorkerContext &&ctx) {
        worker_reconnected_promise.set_value(std::move(ctx));
      });
  auto server_port = master->Start(0);
  RAY_LOG(INFO) << "Master::Started ...";

  auto slave = std::make_shared<Slave>(node_id, server_port);
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  RAY_LOG(INFO) << "Waiting for [worker_connected] ...";
  auto worker_connected_future = worker_connected_promise.get_future();
  auto status = worker_connected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);

  auto worker_context = worker_connected_future.get();
  ASSERT_EQ(node_id, worker_context.node_id);

  // Reset slave
  slave->Restart();
  RAY_LOG(INFO) << "Slave::Restarted ...";

  RAY_LOG(INFO) << "Waiting for [worker_reconnected] ...";
  auto worker_reconnected_future = worker_reconnected_promise.get_future();
  status = worker_reconnected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);

  worker_context = worker_reconnected_future.get();
  ASSERT_EQ(node_id, worker_context.node_id);
}

TEST_F(FailureDetectorTest, WorkerDisconnected) {
  uint64_t node_id = 10010;

  std::promise<fd::WorkerContext> worker_connected_promise;
  std::promise<fd::WorkerContext> worker_disconnected_promise;

  auto master = std::make_shared<Master>();
  master->GetFailureDetector()->OnWorkerConnected([&](fd::WorkerContext &&ctx) {
    worker_connected_promise.set_value(std::move(ctx));
  });
  master->GetFailureDetector()->OnWorkerDisconnected(
      [&](std::vector<fd::WorkerContext> &&ctx) {
        worker_disconnected_promise.set_value(std::move(ctx.front()));
      });
  auto server_port = master->Start(0);
  RAY_LOG(INFO) << "Master::Started ...";

  auto slave = std::make_shared<Slave>(node_id, server_port);
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  RAY_LOG(INFO) << "Waiting for [worker_connected] ...";
  auto worker_connected_future = worker_connected_promise.get_future();
  auto status = worker_connected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);

  auto worker_context = worker_connected_future.get();
  ASSERT_EQ(node_id, worker_context.node_id);

  // Stop slave
  slave->Stop();
  RAY_LOG(INFO) << "Slave::Stopped ...";

  RAY_LOG(INFO) << "Waiting for [worker_disconnected] ...";
  auto worker_disconnected_future = worker_disconnected_promise.get_future();
  status = worker_disconnected_future.wait_for(std::chrono::milliseconds(10000));
  ASSERT_EQ(std::future_status::ready, status);

  worker_context = worker_disconnected_future.get();
  ASSERT_EQ(node_id, worker_context.node_id);

  // Start slave again
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  RAY_LOG(INFO) << "Waiting for [worker_connected] ...";
  worker_connected_promise = std::promise<fd::WorkerContext>();
  worker_connected_future = worker_connected_promise.get_future();
  status = worker_connected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);

  worker_context = worker_connected_future.get();
  ASSERT_EQ(node_id, worker_context.node_id);
}

TEST_F(FailureDetectorTest, MasterConnectedAndDisconnected) {
  uint64_t node_id = 10010;

  std::promise<ip::detail::endpoint> master_connected_promise;
  std::promise<ip::detail::endpoint> master_disconnected_promise;

  auto master = std::make_shared<Master>();
  auto server_port = master->Start(0);
  RAY_LOG(INFO) << "Master::Started ...";

  auto master_endpoint = master->GetEndpoint(server_port);

  auto slave = std::make_shared<Slave>(node_id, server_port);
  slave->GetFailureDetector()->OnMasterConnected(
      [&](const ip::detail::endpoint &endpoint) {
        master_connected_promise.set_value(endpoint);
      });
  slave->GetFailureDetector()->OnMasterDisconnected(
      [&](const ip::detail::endpoint &endpoint) {
        master_disconnected_promise.set_value(endpoint);
      });
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  RAY_LOG(INFO) << "Waiting for [master_connected] ...";
  auto master_connected_future = master_connected_promise.get_future();
  auto status = master_connected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_connected_future.get());

  // Stop master
  master->Stop();
  RAY_LOG(INFO) << "Master::Stopped ...";

  RAY_LOG(INFO) << "Waiting for [master_disconnected] ...";
  auto master_disconnected_future = master_disconnected_promise.get_future();
  status = master_disconnected_future.wait_for(std::chrono::milliseconds(10000));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_disconnected_future.get());
}

TEST_F(FailureDetectorTest, StartSlaveFirst) {
  uint64_t node_id = 10010;
  uint64_t server_port = 12345;

  std::promise<ip::detail::endpoint> master_connected_promise;
  std::promise<ip::detail::endpoint> master_disconnected_promise;

  auto slave = std::make_shared<Slave>(node_id, server_port);
  slave->GetFailureDetector()->OnMasterConnected(
      [&](const ip::detail::endpoint &endpoint) {
        master_connected_promise.set_value(endpoint);
      });
  slave->GetFailureDetector()->OnMasterDisconnected(
      [&](const ip::detail::endpoint &endpoint) {
        master_disconnected_promise.set_value(endpoint);
      });
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  auto master = std::make_shared<Master>();
  ASSERT_EQ(server_port, master->Start(server_port));
  RAY_LOG(INFO) << "Master::Started ...";

  auto master_endpoint = master->GetEndpoint(server_port);

  RAY_LOG(INFO) << "Waiting for [master_connected] ...";
  auto master_connected_future = master_connected_promise.get_future();
  auto status = master_connected_future.wait_for(std::chrono::milliseconds(3000));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_connected_future.get());

  // Stop master
  master->Stop();
  RAY_LOG(INFO) << "Master::Stopped ...";

  RAY_LOG(INFO) << "Waiting for [master_disconnected] ...";
  auto master_disconnected_future = master_disconnected_promise.get_future();
  status = master_disconnected_future.wait_for(std::chrono::milliseconds(10000));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_disconnected_future.get());
}

TEST_F(FailureDetectorTest, MasterPauseAndResume) {
  uint64_t node_id = 10010;

  std::promise<ip::detail::endpoint> master_connected_promise;
  std::promise<ip::detail::endpoint> master_disconnected_promise;

  auto master = std::make_shared<Master>();
  auto server_port = master->Start(0);
  RAY_LOG(INFO) << "Master::Started ...";

  master->GetFailureDetector()->PauseOnMaster();
  RAY_LOG(INFO) << "Master::Paused ...";

  auto master_endpoint = master->GetEndpoint(server_port);

  auto slave = std::make_shared<Slave>(node_id, server_port);
  slave->GetFailureDetector()->OnMasterConnected(
      [&](const ip::detail::endpoint &endpoint) {
        master_connected_promise.set_value(endpoint);
      });
  slave->GetFailureDetector()->OnMasterDisconnected(
      [&](const ip::detail::endpoint &endpoint) {
        master_disconnected_promise.set_value(endpoint);
      });
  slave->Start();
  RAY_LOG(INFO) << "Slave::Started ...";

  auto grace_ms = RayConfig::instance().grace_seconds() * 1000;
  RAY_LOG(INFO) << "Waiting for [master_connected timeout] ...";
  auto master_connected_future = master_connected_promise.get_future();
  auto status = master_connected_future.wait_for(std::chrono::milliseconds(grace_ms));
  ASSERT_EQ(std::future_status::timeout, status);

  master_connected_promise = std::promise<ip::detail::endpoint>();
  master->GetFailureDetector()->ResumeOnMaster();
  RAY_LOG(INFO) << "Master::Resumed ...";
  RAY_LOG(INFO) << "Waiting for [master_connected] ...";
  master_connected_future = master_connected_promise.get_future();
  status = master_connected_future.wait_for(std::chrono::milliseconds(grace_ms));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_connected_future.get());

  master->GetFailureDetector()->PauseOnMaster();
  RAY_LOG(INFO) << "Master::Paused again ...";
  RAY_LOG(INFO) << "Waiting for [master_disconnected] ...";
  auto master_disconnected_future = master_disconnected_promise.get_future();
  status = master_disconnected_future.wait_for(std::chrono::milliseconds(10000));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_disconnected_future.get());

  master_connected_promise = std::promise<ip::detail::endpoint>();
  master->GetFailureDetector()->ResumeOnMaster();
  RAY_LOG(INFO) << "Master::Resumed again ...";
  RAY_LOG(INFO) << "Waiting for [master_connected] ...";
  master_connected_future = master_connected_promise.get_future();
  status = master_connected_future.wait_for(std::chrono::milliseconds(grace_ms));
  ASSERT_EQ(std::future_status::ready, status);
  ASSERT_EQ(master_endpoint, master_connected_future.get());
}

}  // namespace ray

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
