#include "failure_detector.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"

namespace ray {
namespace fd {

FailureDetector::FailureDetector(boost::asio::io_context &ioc)
    : ioc_(ioc), check_timer_(ioc_), send_beacon_timer_(ioc_) {
  process_start_time_ = current_sys_time_ns();
}

void FailureDetector::Start(uint32_t check_interval_seconds,
                            uint32_t beacon_interval_seconds, uint32_t lease_seconds,
                            uint32_t grace_seconds) {
  bool expected = false;
  if (is_started_.compare_exchange_strong(expected, true)) {
    RAY_LOG(INFO) << "[" << __FUNCTION__ << "] "
                  << "fd start with process start_time " << process_start_time_ << " ns";
    check_interval_milliseconds_ = check_interval_seconds * 1000;
    beacon_interval_milliseconds_ = beacon_interval_seconds * 1000;
    lease_milliseconds_ = lease_seconds * 1000;
    grace_milliseconds_ = grace_seconds * 1000;

    // start periodically check job
    auto interval = boost::posix_time::milliseconds(check_interval_milliseconds_);
    check_timer_.expires_from_now(interval);
    check_timer_.async_wait([this](const boost::system::error_code &error) {
      if (error == boost::system::errc::operation_canceled) {
        return;
      }
      RAY_CHECK(!error) << "[CheckRecords] failed! " << error.message();
      CheckRecords();
    });
  }
}

void FailureDetector::Stop() {
  bool expected = true;
  if (is_started_.compare_exchange_strong(expected, false)) {
    send_beacon_timer_.cancel();
    check_timer_.cancel();
  }
}

void FailureDetector::RegisterMaster(const ip::detail::endpoint &target,
                                     uint32_t delay_milliseconds) {
  ioc_.post([=] {
    if (master_) {
      RAY_LOG(INFO) << "[RegisterMaster] node_id: " << node_id_ << ", master["
                    << master_->endpoint.to_string()
                    << "] already registered, just ignore! target: "
                    << target.to_string();
      return;
    }

    master_.reset(new MasterRecord(target, current_sys_time_ms()));
    RAY_LOG(INFO) << "[RegisterMaster] node_id: " << node_id_
                  << ", target: " << target.to_string() << " successfully";

    auto delay = boost::posix_time::milliseconds(delay_milliseconds);
    send_beacon_timer_.expires_from_now(delay);
    send_beacon_timer_.async_wait([this](const boost::system::error_code &error) {
      RAY_CHECK(!error) << "[SendBeacon] node_id: " << node_id_
                        << ", error: " << error.message();
      SendBeacon();
    });
  });
}

void FailureDetector::Report(const ip::detail::endpoint &node_addr, bool is_master,
                             bool is_connected, uint64_t start_time) {
  RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] " << (is_master ? "master " : "worker ")
                   << "[" << node_addr.to_string() << "] "
                   << (is_connected ? "connected" : "disconnected")
                   << (is_master
                           ? ""
                           : (" with start_time " + std::to_string(start_time) + " ns"));
}

/*
                            |--- lease period ----|lease IsExpired, commit suicide
                 |--- lease period ---|
    worker: ---------------------------------------------------------------->
                 \    /     \    /      _\
             beacon ack  beacon ack       x (beacon deliver failed)
                  _\/        _\/
    master: ---------------------------------------------------------------->
                    |---- grace period ----|
                               |--- grace period ----| grace IsExpired, declare worker
   dead
*/

void FailureDetector::CheckRecords() {
  if (!is_started_) {
    return;
  }

  if (master_) {
    bool is_master_expired = false;
    uint64_t now = current_sys_time_ms();
    /*
     * "Check interval" and "send beacon" are interleaved, so we must
     * test if "record will expire before next time we check all the records"
     * in order to guarantee the perfect fd
     */
    if (master_->is_alive &&
        now + check_interval_milliseconds_ - master_->last_beacon_send_time >
            lease_milliseconds_) {
      master_->is_alive = false;
      Report(master_->endpoint, true, false, 0);
      is_master_expired = true;
    }

    if (is_master_expired) {
      if (on_master_disconnected_) {
        on_master_disconnected_(master_->endpoint);
      }
    }
  }

  {
    auto now = current_sys_time_ms();
    // process recv record, for server
    std::vector<WorkerContext> expired_workers;
    for (auto &entry : workers_) {
      auto &worker = entry.second;
      if (worker.is_alive && now - worker.last_beacon_recv_time > grace_milliseconds_) {
        WorkerContext context;
        context.node_id = worker.node_id;
        context.endpoint = worker.endpoint;
        context.beacon_context = worker.last_received_context;
        expired_workers.emplace_back(context);

        worker.is_alive = false;
        Report(worker.endpoint, false, false, worker.process_start_time);
      }
    }
    /*
     * The worker disconnected event also need to be under protection of the _lock
     */
    if (!expired_workers.empty()) {
      if (on_worker_disconnected_) {
        on_worker_disconnected_(std::move(expired_workers));
      }
    }
  }

  auto interval = boost::posix_time::milliseconds(check_interval_milliseconds_);
  check_timer_.expires_from_now(interval);
  check_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      return;
    }
    RAY_CHECK(!error) << "[CheckRecords] failed! " << error.message();
    CheckRecords();
  });
}

void FailureDetector::OnPingInternal(const rpc::BeaconMsg &beacon, rpc::BeaconAck *ack) {
  bool pause_on_master = pause_on_master_.load(std::memory_order_relaxed);

  ack->set_time(beacon.time());
  ack->set_this_node(beacon.to_addr());
  ack->set_primary_node(endpoint_to_uint64(primary_endpoint_));
  ack->set_is_master(true);
  ack->set_is_paused(pause_on_master);

  uint64_t now = current_sys_time_ms();
  auto node_id = beacon.node_id();
  auto node_endpoint = endpoint_from_uint64(beacon.from_addr());

  auto itr = workers_.find(node_id);
  if (itr == workers_.end()) {
    // no connect if paused
    if (pause_on_master) {
      return;
    }

    // create new entry for node
    WorkerRecord record(node_id, node_endpoint, now);
    record.is_alive = true;
    record.process_start_time = beacon.process_start_time();
    record.last_received_context = beacon.context();

    workers_.insert(std::make_pair(node_id, record));
    Report(node_endpoint, false, true, beacon.process_start_time());

    WorkerContext context;
    context.node_id = node_id;
    context.endpoint = node_endpoint;
    context.beacon_context = beacon.context();
    if (on_worker_connected_) {
      on_worker_connected_(std::move(context));
    }
  } else if (now >= itr->second.last_beacon_recv_time) {
    if (pause_on_master && !itr->second.is_alive) {
      return;
    }

    bool restarted = false;
    if (itr->second.process_start_time < beacon.process_start_time()) {
      RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id << ", node "
                       << node_endpoint.to_string()
                       << " reconnect after restart with start_time "
                       << itr->second.process_start_time << " -> "
                       << beacon.process_start_time();
      itr->second.process_start_time = beacon.process_start_time();
      restarted = true;
    } else if (itr->second.process_start_time > beacon.process_start_time()) {
      RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id
                       << ", receive a process_start_time"
                          " that is smaller than cached process_start_time, ("
                       << beacon.process_start_time() << " < "
                       << itr->second.process_start_time << "), just ignore.";
      return;
    }

    // update last_beacon_recv_time
    itr->second.last_beacon_recv_time = now;
    itr->second.last_received_context = beacon.context();

    WorkerContext context;
    context.node_id = node_id;
    context.endpoint = node_endpoint;
    context.beacon_context = beacon.context();
    if (!itr->second.is_alive) {
      itr->second.is_alive = true;
      Report(node_endpoint, false, true, itr->second.process_start_time);
      if (on_worker_connected_) {
        on_worker_connected_(std::move(context));
      }
    } else if (restarted) {
      if (on_worker_restarted_) {
        on_worker_restarted_(std::move(context));
      }
    }
  }
}

void FailureDetector::EndPing(const Status &err, const rpc::BeaconAck &ack, void *) {
  EndPingInternal(err, ack);
}

bool FailureDetector::EndPingInternal(const Status &err, const rpc::BeaconAck &ack) {
  /*
   * the caller of the EndPingInternal should lock necessarily!!!
   */
  uint64_t beacon_send_time = ack.time();
  auto master_endpoint = endpoint_from_uint64(ack.this_node());
  uint64_t now = current_sys_time_ms();

  if (!(master_->endpoint == master_endpoint)) {
    RAY_LOG(ERROR) << "[" << __FUNCTION__ << "] node_id: " << node_id_
                   << ", received beacon ack from unknown master, just ignore! "
                   << "remote_master[" << master_endpoint.to_string()
                   << "], local_worker[" << primary_endpoint_.to_string() << "]";
    return false;
  }

  if (beacon_send_time < master_->last_beacon_send_time) {
    // out-dated beacon acks, do nothing
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id_
                     << ", just ignore outdated beacon ack!";
    return false;
  }

  // now the ack is applicable
  if (!err.ok()) {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id_
                     << ", ping master(" << master_endpoint.to_string() << ") failed"
                     << ", err = " << err;
    return true;
  }

  if (ack.is_paused()) {
    RAY_LOG(WARNING) << "[" << __FUNCTION__ << "] node_id: " << node_id_
                     << ", ping master(" << master_endpoint.to_string() << ") failed, "
                     << "as master is paused!";
    return true;
  }

  // update last_beacon_send_time
  master_->last_beacon_send_time = beacon_send_time;
  if (!master_->is_alive && now - master_->last_beacon_send_time <= lease_milliseconds_) {
    // report master connected
    Report(master_endpoint, true, true, 0);
    master_->is_alive = true;
    if (ack.is_master()) {
      if (on_master_connected_) {
        on_master_connected_(master_endpoint);
      }
    }
  }
  return true;
}

rpc::FailureDetectorAsioClient *FailureDetector::GetFDClient() {
  if (fd_client_ == nullptr) {
    auto address = master_->endpoint.address().to_string();
    auto port = master_->endpoint.port();
    fd_client_.reset(new rpc::FailureDetectorAsioClient(address, port, ioc_));
  }
  return fd_client_.get();
}

void FailureDetector::ResetFDClient() { fd_client_.reset(); }

void FailureDetector::SendBeacon() {
  rpc::BeaconMsg beacon;

  auto primary_endpoint = endpoint_to_uint64(primary_endpoint_);
  beacon.set_node_id(node_id_ ? node_id_ : primary_endpoint);
  beacon.set_time(current_sys_time_ms());
  beacon.set_from_addr(primary_endpoint);
  beacon.set_to_addr(endpoint_to_uint64(master_->endpoint));
  beacon.set_process_start_time(process_start_time_);
  if (beacon_context_generator_) {
    beacon.set_context(beacon_context_generator_());
  }

  RAY_LOG(DEBUG) << "[" << __FUNCTION__ << "] node_id: " << node_id_
                 << ", send ping message, from["
                 << endpoint_from_uint64(beacon.from_addr()).to_string() << "], to["
                 << endpoint_from_uint64(beacon.to_addr()).to_string() << "], time["
                 << beacon.time() << "]";

  auto is_done = std::make_shared<bool>(false);
  auto cb = [=](const Status &status, const rpc::BeaconAck &reply) {
    if (*is_done) {
      return;
    }
    *is_done = true;

    if (status.ok()) {
      EndPing(status, reply, nullptr);
      return;
    }

    if (status.IsTimeout()) {
      rpc::BeaconAck ack;
      ack.set_time(beacon.time());
      ack.set_this_node(beacon.to_addr());
      ack.set_primary_node(0);
      ack.set_is_master(false);
      ack.set_is_paused(false);
      EndPing(status, ack, nullptr);
      return;
    }
  };

  auto interval = boost::posix_time::milliseconds(beacon_interval_milliseconds_);

  if (auto fd_client = GetFDClient()) {
    auto timer = std::make_shared<boost::asio::deadline_timer>(ioc_);
    timer->expires_from_now(interval);
    timer->async_wait([this, cb](const boost::system::error_code &error) {
      if (error == boost::system::errc::operation_canceled) {
        return;
      }
      RAY_CHECK(!error) << "[SendBeacon] failed! node_id: " << node_id_
                        << ", error: " << error.message();
      cb(Status::Timeout("Send ping timeout"), rpc::BeaconAck());
    });

    auto status = fd_client->Ping(
        beacon, [cb, timer](const Status &status, const rpc::BeaconAck &reply) {
          timer->cancel();
          cb(status, reply);
        });

    if (status.IsInvalid()) {
      RAY_LOG(WARNING) << "[SendBeacon] failed! node_id: " << node_id_
                       << ", error: " << status;
      ResetFDClient();
    }
  }

  send_beacon_timer_.expires_from_now(interval);
  send_beacon_timer_.async_wait([this](const boost::system::error_code &error) {
    if (error == boost::system::errc::operation_canceled) {
      return;
    }
    RAY_CHECK(!error) << "[SendBeacon] failed! node_id: " << node_id_
                      << ", error: " << error.message();
    SendBeacon();
  });
}

}  // namespace fd
}  // namespace ray
