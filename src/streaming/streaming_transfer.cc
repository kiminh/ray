#include "ray/util/logging.h"

#include "streaming_transfer.h"

namespace ray {
namespace streaming {
std::queue<std::shared_ptr<StreamingMessage>> StreamingDefaultBlockedQueue::message_store_;
std::mutex StreamingDefaultBlockedQueue::store_mutex_;

size_t StreamingDefaultBlockedQueue::Size() {
  std::unique_lock<std::mutex> lock(store_mutex_);
  return message_store_.size();
}

std::shared_ptr<StreamingMessage> StreamingDefaultBlockedQueue::Front() {
  std::unique_lock<std::mutex> lock(store_mutex_);
  return message_store_.front();
}

bool StreamingDefaultBlockedQueue::Empty() {
  std::unique_lock<std::mutex> lock(store_mutex_);
  return message_store_.empty();
}

bool StreamingDefaultBlockedQueue::Push(std::shared_ptr<StreamingMessage> msg) {
  std::unique_lock<std::mutex> lock(store_mutex_);
  message_store_.push(msg);
  return true;
}

void StreamingDefaultBlockedQueue::Pop() {
  std::unique_lock<std::mutex> lock(store_mutex_);
  message_store_.pop();
}

StreamingStatus StreamingDefaultProduceTransfer::InitProducer(StreamingChannelConfig &channel_config) {
  RAY_LOG(INFO) << "Init Default Transfer Producer";
  return StreamingStatus::OK;
};

StreamingStatus StreamingDefaultProduceTransfer::ProduceMessage(
  StreamingChannelInfo &channel_info,
  std::shared_ptr<StreamingMessage> msg) {
  StreamingDefaultBlockedQueue::Push(msg);
  RAY_LOG(INFO) << "Produce Message";
  return StreamingStatus::OK;
}

StreamingStatus StreamingDefaultProduceTransfer::DestoryTransfer() {
  if (is_init_) {
    RAY_LOG(INFO) << "Destory Default Transfer Producer";
    is_init_ = false;
  }
}
StreamingDefaultProduceTransfer::~StreamingDefaultProduceTransfer() {
  DestoryTransfer();
}

StreamingStatus StreamingDefaultConsumeTransfer::InitConsumer(StreamingChannelConfig &channel_config) {
  RAY_LOG(INFO) << "Init Default Transfer Consumer";
  return StreamingStatus::OK;
};

StreamingStatus StreamingDefaultConsumeTransfer::ConsumeMessage(
  StreamingChannelInfo &channel_info,
  std::shared_ptr<StreamingMessage> &msg) {
  if (!StreamingDefaultBlockedQueue::Empty()) {
    msg = StreamingDefaultBlockedQueue::Front();
    StreamingDefaultBlockedQueue::Pop();
  }
  RAY_LOG(INFO) << "Consume Message";
  return StreamingStatus::OK;
}

StreamingStatus StreamingDefaultConsumeTransfer::DestoryTransfer() {
  if (is_init_) {
    RAY_LOG(INFO) << "Destory Default Transfer Consumer";
    is_init_ = false;
  }
}

StreamingDefaultConsumeTransfer::~StreamingDefaultConsumeTransfer() {
  DestoryTransfer();
}

}
}

