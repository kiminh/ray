#pragma once

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include <ray/protobuf/failure_detector.pb.h>
#include <boost/asio.hpp>
#include "ray/rpc/failure_detector/failure_detector_client.h"
using namespace boost::asio;
using namespace boost::asio::ip::detail;

namespace ray {
namespace fd {

struct WorkerContext {
  uint64_t node_id = 0;
  ip::detail::endpoint endpoint;
  std::string beacon_context;
};

class FailureDetector {
 public:
  explicit FailureDetector(boost::asio::io_context &ioc);
  ~FailureDetector() = default;

  virtual void EndPing(const Status &err, const rpc::BeaconAck &ack, void *context);

 public:
  void Start(uint32_t check_interval_seconds, uint32_t beacon_interval_seconds,
             uint32_t lease_seconds, uint32_t grace_seconds);

  void Stop();

  void RegisterMaster(const ip::detail::endpoint &target, uint32_t delay_milliseconds);

  uint32_t GetLeaseMillisecond() const { return lease_milliseconds_; }

  uint32_t GetGraceMillisecond() const { return grace_milliseconds_; }

  void RegisterBeaconContextGenerator(
      std::function<std::string()> &&beacon_context_generator) {
    beacon_context_generator_ = std::move(beacon_context_generator);
  }

  void SetNodeId(uint64_t node_id) { node_id_ = node_id; }

  void SetPrimaryEndpoint(const ip::detail::endpoint &endpoint) {
    primary_endpoint_ = endpoint;
  }

  void PauseOnMaster() { pause_on_master_.store(true, std::memory_order_relaxed); }

  void ResumeOnMaster() { pause_on_master_.store(false, std::memory_order_relaxed); }

  void OnMasterDisconnected(std::function<void(const ip::detail::endpoint &)> &&handler) {
    on_master_disconnected_ = std::move(handler);
  }

  void OnMasterConnected(std::function<void(const ip::detail::endpoint &)> &&handler) {
    on_master_connected_ = std::move(handler);
  }

  void OnWorkerDisconnected(
      std::function<void(std::vector<WorkerContext> &&)> &&handler) {
    on_worker_disconnected_ = std::move(handler);
  }

  void OnWorkerConnected(std::function<void(WorkerContext &&)> &&handler) {
    on_worker_connected_ = std::move(handler);
  }

  void OnWorkerRestartedWithinLease(std::function<void(WorkerContext &&)> &&handler) {
    on_worker_restarted_ = std::move(handler);
  }

 protected:
  // return false to drop the reply
  void OnPingInternal(const rpc::BeaconMsg &beacon, rpc::BeaconAck *ack);

  // return false when the ack is not applicable
  bool EndPingInternal(const Status &err, const rpc::BeaconAck &ack);

  void Report(const ip::detail::endpoint &node_addr, bool is_master, bool is_connected,
              uint64_t start_time);

 private:
  void CheckRecords();
  void SendBeacon();
  rpc::FailureDetectorAsioClient *GetFDClient();

 private:
  class MasterRecord {
   public:
    ip::detail::endpoint endpoint;
    uint64_t last_beacon_send_time = 0;
    bool is_alive = false;

    // masters are always considered *disconnected* initially which is ok even when master
    // thinks workers are connected
    MasterRecord(const ip::detail::endpoint &endpoint,
                 uint64_t last_send_time_for_beacon_with_ack) {
      this->endpoint = endpoint;
      this->last_beacon_send_time = last_send_time_for_beacon_with_ack;
    }
  };

  class WorkerRecord {
   public:
    uint64_t node_id = 0;
    ip::detail::endpoint endpoint;
    uint64_t last_beacon_recv_time = 0;
    bool is_alive = true;
    uint64_t process_start_time = 0;
    std::string last_received_context;

    // workers are always considered *connected* initially which is ok even when workers
    // think master is disconnected
    WorkerRecord(uint64_t node_id, const ip::detail::endpoint &node_endpoint,
                 uint64_t last_beacon_recv_time) {
      this->node_id = node_id;
      this->endpoint = node_endpoint;
      this->last_beacon_recv_time = last_beacon_recv_time;
    }
  };

 private:
  std::unique_ptr<MasterRecord> master_;
  std::unordered_map<uint64_t, WorkerRecord> workers_;

  uint32_t beacon_interval_milliseconds_ = 0;
  uint32_t check_interval_milliseconds_ = 0;
  uint32_t lease_milliseconds_ = 0;
  uint32_t grace_milliseconds_ = 0;
  std::atomic_bool is_started_{false};
  ip::detail::endpoint primary_endpoint_;

  boost::asio::io_context &ioc_;
  boost::asio::deadline_timer check_timer_;
  boost::asio::deadline_timer send_beacon_timer_;

  uint64_t process_start_time_ = 0;
  uint64_t node_id_ = 0;
  std::function<std::string()> beacon_context_generator_;
  std::atomic_bool pause_on_master_{false};

  std::unique_ptr<rpc::FailureDetectorAsioClient> fd_client_;

  std::function<void(const ip::detail::endpoint &)> on_master_connected_;
  std::function<void(const ip::detail::endpoint &)> on_master_disconnected_;

  std::function<void(std::vector<WorkerContext> &&)> on_worker_disconnected_;
  std::function<void(WorkerContext &&)> on_worker_connected_;
  std::function<void(WorkerContext &&)> on_worker_restarted_;
};

}  // namespace fd
}  // namespace ray
