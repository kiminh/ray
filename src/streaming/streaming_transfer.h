//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_TRANSFER_H
#define RAY_STREAMING_STREAMING_TRANSFER_H
#include <iostream>
#include <mutex>
#include <unordered_map>
#include <condition_variable>

#include "streaming_constant.h"
#include "streaming_message.h"
#include "streaming_channel.h"

#include <queue>

namespace ray {
namespace streaming {
class StreamingChannelInfo;

class StreamingTransfer {
 public:
  StreamingTransfer() : is_init_(false) {};
  virtual ~StreamingTransfer() {};
  virtual StreamingStatus InitTransfer(StreamingChannelConfig &channel_config) = 0;
  virtual StreamingStatus DestoryTransfer() = 0;
 protected:
  bool is_init_;
};

class StreamingProduceTransfer : public StreamingTransfer {
 public:
  StreamingProduceTransfer() {}
  virtual StreamingStatus ProduceMessage(StreamingChannelInfo &channel_info,
                                         std::shared_ptr<StreamingMessage> msg) = 0;

  virtual StreamingStatus InitTransfer(StreamingChannelConfig &channel_config) {
    is_init_ = true;
    return this->InitProducer(channel_config);
  };

  virtual ~StreamingProduceTransfer() {}

 protected:
  virtual StreamingStatus InitProducer(StreamingChannelConfig &channel_config) = 0;

};

class StreamingConsumeTransfer : public StreamingTransfer {
 public:
  StreamingConsumeTransfer() {}
  virtual StreamingStatus ConsumeMessage(StreamingChannelInfo &channel_info,
                                         std::shared_ptr<StreamingMessage> &msg) = 0;

  virtual StreamingStatus InitTransfer(StreamingChannelConfig &channel_config) {
    is_init_ = true;
    return this->InitConsumer(channel_config);
  };
  virtual ~StreamingConsumeTransfer() {}

 protected:
  virtual StreamingStatus InitConsumer(StreamingChannelConfig &channel_config) = 0;

};


class StreamingDefaultBlockedQueue {
 public:
  bool Push(std::shared_ptr<StreamingMessage> msg);
  std::shared_ptr<StreamingMessage> Front();
  size_t Size();
  bool Empty();
  void Pop();
 private:
  std::queue<std::shared_ptr<StreamingMessage>> message_queue_;
  std::mutex queue_mutex_;
};

class StreamingDefaultStore {
 public:
  static bool Push(StreamingChannelIndex &index, std::shared_ptr<StreamingMessage> msg);
  static void Pop(std::vector<StreamingChannelIndex> &indexes, std::shared_ptr<StreamingMessage> &msg);
 private:
  static bool Empty(std::vector<StreamingChannelIndex> &indexes);
 private:
  static std::unordered_map<StreamingChannelIndex, StreamingDefaultBlockedQueue> message_store_;
  static std::mutex store_mutex_;
  static std::condition_variable store_cv_;
};

// StreamingDefaultTransfer
// Store message in memory stl queue of single process for prototype
class StreamingDefaultProduceTransfer : public StreamingProduceTransfer {
 public:
  StreamingDefaultProduceTransfer() : StreamingProduceTransfer() {}

  virtual ~StreamingDefaultProduceTransfer();

  StreamingStatus DestoryTransfer() override;

 protected:
  StreamingStatus InitProducer(StreamingChannelConfig &channel_config) override;

  StreamingStatus ProduceMessage(StreamingChannelInfo &channel_info, std::shared_ptr<StreamingMessage> msg) override;

};

class StreamingDefaultConsumeTransfer : public StreamingConsumeTransfer {
 public:
  StreamingDefaultConsumeTransfer() : StreamingConsumeTransfer() {}

  virtual ~StreamingDefaultConsumeTransfer();

  StreamingStatus DestoryTransfer() override;

 protected:
  StreamingStatus InitConsumer(StreamingChannelConfig &channel_config) override;

  StreamingStatus ConsumeMessage(StreamingChannelInfo &channel_info, std::shared_ptr<StreamingMessage> &msg) override;
 private:
  std::vector<StreamingChannelIndex> channel_indexes_;
};

}
}
#endif //RAY_STREAMING_STREAMING_TRANSFER_H
