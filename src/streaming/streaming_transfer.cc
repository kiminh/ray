#include "ray/util/logging.h"

#include "streaming_transfer.h"

namespace ray {
namespace streaming {
std::unordered_map<StreamingChannelIndex, StreamingDefaultBlockedQueue> StreamingDefaultStore::message_store_;
std::mutex StreamingDefaultStore::store_mutex_;
std::condition_variable StreamingDefaultStore::store_cv_;

size_t StreamingDefaultBlockedQueue::Size() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  return message_queue_.size();
}

std::shared_ptr<StreamingMessage> StreamingDefaultBlockedQueue::Front() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  return message_queue_.front();
}

bool StreamingDefaultBlockedQueue::Empty() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  return message_queue_.empty();
}

bool StreamingDefaultBlockedQueue::Push(std::shared_ptr<StreamingMessage> msg) {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  message_queue_.push(msg);
  return true;
}

void StreamingDefaultBlockedQueue::Pop() {
  std::unique_lock<std::mutex> lock(queue_mutex_);
  message_queue_.pop();
}

bool StreamingDefaultStore::Push(ray::streaming::StreamingChannelIndex &index, std::shared_ptr<StreamingMessage> msg) {
  message_store_[index].Push(msg);
  store_cv_.notify_one();
  return true;
}

void StreamingDefaultStore::Pop(std::shared_ptr<StreamingMessage> &msg) {
  std::unique_lock<std::mutex> lock(store_mutex_);
  store_cv_.wait(lock,[]() {
   return !StreamingDefaultStore::Empty();
  });

  for(auto &it : message_store_) {
    if (!it.second.Empty()) {
      msg = it.second.Front();
      it.second.Pop();
    }
  }
}

bool StreamingDefaultStore::Empty() {
  size_t store_msg_cnt_ = 0;
  for(auto &it : message_store_) {
    store_msg_cnt_ += it.second.Size();
  }
  return store_msg_cnt_ == 0;
}

StreamingStatus StreamingDefaultProduceTransfer::InitProducer(StreamingChannelConfig &channel_config) {
  RAY_LOG(INFO) << "Init Default Transfer Producer";
  return StreamingStatus::OK;
};

StreamingStatus StreamingDefaultProduceTransfer::ProduceMessage(
  StreamingChannelInfo &channel_info,
  std::shared_ptr<StreamingMessage> msg) {
  StreamingDefaultStore::Push(channel_info.Index(), msg);
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
  StreamingDefaultStore::Pop(msg);
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

