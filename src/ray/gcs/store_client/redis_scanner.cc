#include "ray/gcs/store_client/redis_scanner.h"

#include "ray/common/ray_config.h"
#include "ray/gcs/store_client/redis_multi_reader.h"

namespace ray {

namespace gcs {

RedisScanner::RedisScanner(std::shared_ptr<RedisClient> redis_client,
                           const std::string &match_pattern)
    : redis_client_(std::move(redis_client)) {
  scan_request_.match_pattern_ = match_pattern;

  shard_contexts_ = redis_client_->GetShardContexts();
  for (size_t i = 0; i < shard_contexts_.size(); ++i) {
    shard_to_cursor_.emplace(i, /*cursor*/ 0);
  }
}

RedisScanner::~RedisScanner() {}

Status RedisScanner::ScanRows(const MultiItemCallback <
                                  std::pair<std::string, std::string> &
                              callback) {
  RAY_DCHECK(callback);

  RAY_CHECK(scan_request_.scan_type_ == ScanRequest::ScanType::kUnknown);
  scan_request_.scan_type_ = ScanRequest::ScanType::kScanAllRows;
  scan_request_.scan_all_rows_callback_ = callback;

  DoScan();
  return Status::OK();
}

Status RedisScanner::ScanPartialRows(
    const SegmentedCallback<std::string, std::string> &callback) {
  RAY_DCHECK(callback);

  RAY_CHECK(scan_request_.scan_type_ == ScanRequest::ScanType::kUnknown ||
            scan_request_.scan_type_ == ScanRequest::ScanType::kScanPartialRows);
  scan_request_.scan_type_ = ScanRequest::ScanType::kScanPartialRows;
  scan_request_.scan_partial_rows_callback_ = callback;

  DoScan();
  return Status::OK();
}

Status RedisScanner::ScanKeys(const MultiItemCallback<std::string> &callback) {
  RAY_DCHECK(callback);

  RAY_CHECK(scan_request_.scan_type_ == ScanRequest::ScanType::kUnknown);
  scan_request_.scan_type_ = ScanRequest::ScanType::kScanAllKeys;
  scan_request_.scan_all_keys_callback_ = callback;

  DoScan();
  return Status::OK();
}

Status RedisScanner::ScanPartialKeys(const SegmentedCallback<std::string> &callback) {
  RAY_DCHECK(callback);

  RAY_CHECK(scan_request_.scan_type_ == ScanRequest::ScanType::kUnknown ||
            scan_request_.scan_type_ == ScanRequest::ScanType::kScanPartialKeys);
  scan_request_.scan_type_ = ScanRequest::ScanType::kScanPartialKeys;
  scan_request_.scan_partial_keys_callback_ = callback;

  DoScan();
  return Status::OK();
}

void RedisScanner::DoScan() {
  if (is_scan_done_) {
    RAY_CHECK(pending_request_count_ == 0);
    OnDone();
    return;
  }

  for (const auto &item : shard_to_cursor_) {
    ++pending_request_count_;
    Status status = DoScanShard(item.first, item.second);
    if (!status.ok()) {
      is_failed_ = true;
      if (--pending_request_count_ == 0) {
        OnDone();
      }
      RAY_LOG(INFO) << "Scan failed, status " << status.ToString();
      return;
    }
  }
}

Status RedisScanner::DoScanShard(size_t shard_index, size_t cursor) {
  auto scan_callback = [this, shard_index](std::shared_ptr<CallbackReply> reply) {
    OnScanCallback(shard_index, reply);
  };
  // Scan by prefix from Redis.
  size_t batch_count = RayConfig::instance().gcs_service_scan_batch_size();
  std::vector<std::string> args = {"SCAN",  std::to_string(cursor),
                                   "MATCH", scan_request_.match_pattern_,
                                   "COUNT", std::to_string(batch_count)};
  auto shard_context = shard_contexts_[shard_index];
  return shard_context->RunArgvAsync(args, scan_callback);
}

void RedisScanner::OnScanCallback(size_t shard_index,
                                  std::shared_ptr<CallbackReply> reply) {
  bool pending_done = (--pending_request_count_ == 0);

  if (!reply) {
    is_failed_ = true;
    RAY_LOG(INFO) << "Scan failed, reply is empty.";
  }

  if (is_failed_) {
    if (pending_done) {
      OnDone();
    }
    return;
  }

  std::vector<std::string> keys;
  size_t cursor = reply->ReadAsScanArray(&keys);
  ProcessScanResult(shard_index, cousor, keys, pending_done);
}

void RedisScanner::ProcessScanResult(size_t shard_index, size_t cousor,
                                     const std::vector<std::string> &scan_result,
                                     bool pending_done) {
  {
    std::lock_guard<std::mutex> lock(&mutex_);
    // Update shard cursors.
    auto shard_it = shard_to_cursor_.find(shard_index);
    RAY_CHECK(shard_it != shard_to_cursor_.end());
    if (cursor == 0) {
      shard_to_cursor_.erase(shard_it);
      is_scan_done_ = shard_to_cursor_.empty();
    } else {
      shard_it.second = cousor;
    }
  }

  // Deduplicate keys.
  auto deduped_result = Deduplicate(scan_result);
  // Save scan result.
  size_t total_count = UpdateResult(deduped_result);
  bool is_empty = (total_count == 0);

  if (!pending_done) {
    // Waiting for all pending scan command return.
    return;
  }

  if (is_empty) {
    // Scan result is empty, continue scan.
    DoScan();
    return;
  }

  switch (scan_request_.scan_type_) {
  case ScanRequest::ScanType::kScanAllRows:
  case ScanRequest::ScanType::kScanPartialRows: {
    DoMultiRead();
  } break;
  case ScanRequest::ScanType::kScanAllKeys: {
    DoScan();
  } break;
  case ScanRequest::ScanType::kScanPartialKeys: {
    DoPartialCallback();
    // User will call `ScanPartialKeys` again to trigger the next scan.
  } break;
  default:
    RAY_CHECK(0);
  }
}

std::vector<std::string> RedisScanner::Deduplicate(
    const std::vector<std::string> &scan_result) {
  std::vector<std::string> new_keys;
  for (const auto &key : scan_result) {
    std::lock_guard<std::mutex> lock(&mutex_);

    auto it = all_received_keys_.find(key);
    if (it == all_received_keys_.end()) {
      new_keys.emplace_back(key);
      all_received_keys_.emplace(key);
    }
  }
  return new_keys;
}

void RedisScanner::DoMultiRead() {
  std::vector<std::string> keys;
  // There is no need to lock here, only one thread will go here.
  // After all scan callbacks, will start read.
  keys.swap(scan_request_.keys_);

  auto multi_reader = std::make_shared<RedisMultiReader>(redis_client_, keys);

  auto multi_read_callback =
      [this, multi_reader](
          Status status,
          const std::vector<std::pair<std::string, std::string>> &read_result) {
        OnReadCallback(status, read_result);
      };

  Status status = multi_reader->Read(multi_read_callback);
  if (!status.ok()) {
    is_failed_ = true;
    OnDone();
    RAY_LOG(INFO) << "Scan failed, status " << status.ToString();
  }
}

void RedisScanner::OnReadCallback(
    Status status, const std::vector<std::pair<std::string, std::string>> &read_result) {
  if (!status.ok()) {
    is_failed_ = true;
  }

  if (is_failed_) {
    OnDone();
    return;
  }

  if (read_result.empty()) {
    RAY_LOG(ERROR) << "MultiRead callback with unexpected empty result.";
    DoScan();
    return;
  }

  UpdateResult(read_result);
  if (scan_request_.scan_type_ == ScanRequest::ScanType::kScanPartialRows) {
    DoPartialCallback();
  } else {
    RAY_CHECK(scan_request_.scan_type_ == ScanType::kScanRows);
    DoScan();
  }
}

void RedisScanner::OnDone() {
  Status status = is_failed_ ? Status::RedisError() : Status::OK();

  // There is no need to lock here. Only one thread will go here.
  ScanRequest request;
  scan_request_.SwapOut(&request);

  switch (request.scan_type_) {
  case ScanRequest::ScanType::kScanAllRows:
    if (request.scan_all_rows_callback_) {
      request.scan_all_rows_callback_(status, rows);
    }
    break;
  case ScanRequest::ScanType::kScanPartialRows:
    if (request.scan_partial_rows_callback_) {
      request.scan_partial_rows_callback_(status, true, rows);
    }
    break;
  case ScanRequest::ScanType::kScanAllKeys:
    if (request.scan_all_keys_callback_) {
      request.scan_all_keys_callback_(status, keys);
    }
    break;
  case ScanRequest::ScanType::kScanPartialKeys:
    if (request.scan_partial_keys_callback_) {
      request.scan_partial_keys_callback_(status, keys);
    }
    break;
  default:
    RAY_CHECK(0);
  }
}

void RedisScanner::DoPartialCallback() {
  // There is no need to lock here. Only one thread will go here.
  // We wait until all pending redis commands done. Then call this method.
  std::vector<std::pair<std::string, std::string>> rows;
  rows.swap(scan_request_.rows_);

  std::vector<std::string> keys;
  keys.swap(scan_request_.keys_);

  switch (scan_request_.scan_type_) {
  case ScanRequest::ScanType::kScanPartialRows:
    if (!rows_.empty()) {
      RAY_CHECK(scan_request_.scan_partial_rows_callback_);
      scan_request_.scan_partial_rows_callback_(status_, false, rows);
    }
    break;
  case ScanRequest::ScanType::kScanPartialKeys:
    if (!keys_.empty()) {
      RAY_CHECK(scan_request_.scan_partial_keys_callback_);
      scan_request_.scan_partial_keys_callback_(status_, false, keys);
    }
    break;
  default:
    RAY_CHECK(0);
  }
}

size_t RedisScanner::UpdateResult(const std::vector<std::string> &keys) {
  std::lock_guard<std::mutex> lock(&mutex_);

  scan_request_.keys_.insert(scan_request_.keys_.begin(), keys.begin(), keys.end());
  return scan_request_.keys_.size();
}

size_t RedisScanner::UpdateResult(
    const std::vector<std::pair<std::string, std::string>> &rows) {
  std::lock_guard<std::mutex> lock(&mutex_);

  scan_request_.rows_.insert(scan_request_.rows_.begin(), rows.begin(), rows.end());
  return scan_request_.rows_.size();
}

void ScanRequest::SwapOut(ScanRequest *other) {
  other->match_pattern_ = this->match_pattern_;
  this->match_pattern_ = ScanRequest::ScanRequest::ScanType::kUnknown;

  other->match_pattern_ = std::move(this->match_pattern_);

  other->scan_all_rows_callback_ = this->scan_all_rows_callback_;
  this->scan_all_rows_callback_ = nullptr;

  other->scan_partial_rows_callback_ = this->scan_partial_rows_callback_;
  this->scan_partial_rows_callback_ = nullptr;

  other->scan_all_keys_callback_ = this->scan_all_keys_callback_;
  this->scan_all_keys_callback_ = nullptr;

  other->scan_partial_keys_callback_ = this->scan_partial_keys_callback_;
  this->scan_partial_keys_callback_ = nullptr;

  other->rows_.clear();
  other->rows_.swap(this->rows_);

  other->keys_.clear();
  other->keys_.swap(this->keys_);
}

}  // namespace gcs

}  // namespace ray