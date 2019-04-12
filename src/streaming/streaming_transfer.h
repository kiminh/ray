//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_TRANSFER_H
#define RAY_STREAMING_STREAMING_TRANSFER_H
#include <iostream>
#include <mutex>

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
  static bool Push(std::shared_ptr<StreamingMessage> msg);
  static std::shared_ptr<StreamingMessage> Front();
  static size_t Size();
  static bool Empty();
  static void Pop();
 private:
  static std::queue<std::shared_ptr<StreamingMessage>> message_store_;
  static std::mutex store_mutex_;
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
};

}
}
#endif //RAY_STREAMING_STREAMING_TRANSFER_H
