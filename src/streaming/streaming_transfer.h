//
// Created by ashione on 2019/4/1.
//

#ifndef STREAMING_PROTOTYPE_STREAMING_TRANSFORMATION_H
#define STREAMING_PROTOTYPE_STREAMING_TRANSFORMATION_H
#include <iostream>

#include "streaming_constant.h"
#include "streaming_message.h"
#include "streaming_channel.h"

#include <queue>

enum class StreamingTransferRole : uint8_t {
  StreamingProduer = 0,
  StreamingConsumer = 1
};

class StreamingChannelInfo;

// divided into two classes
class StreamingTransfer{
 public:
  StreamingTransfer() : is_init_(false) {}
  virtual StreamingStatus ProduceMessage(StreamingChannelInfo &channel_info,
                                         std::shared_ptr<StreamingMessage> msg) = 0;
  virtual StreamingStatus ConsumeMessage(StreamingChannelInfo &channle_info,
                                         std::shared_ptr<StreamingMessage> &msg) = 0;

  virtual StreamingStatus InitTransfer(StreamingChannelConfig &channel_config,
                                       StreamingTransferRole role) {
    is_init_ = true;
    role_ = role;
    if (StreamingTransferRole::StreamingProduer == role_) {
      return this->InitProducer(channel_config);
    } else {
      return this->InitConsumer(channel_config);
    }
  };

  virtual StreamingStatus DestoryTransfer() {
    if (!is_init_) { return StreamingStatus::OK; }
    if (StreamingTransferRole::StreamingProduer == role_) {
      return this->DestoryProducer();
    } else {
      return this->DestoryConsumer();
    }
  }

  virtual ~StreamingTransfer() {}

 protected:
  virtual StreamingStatus InitProducer(StreamingChannelConfig &channel_config) = 0;
  virtual StreamingStatus InitConsumer(StreamingChannelConfig &channel_config) = 0;
  virtual StreamingStatus DestoryProducer() = 0;
  virtual StreamingStatus DestoryConsumer() = 0;


 protected:
  StreamingTransferRole role_;
  bool is_init_;

};

// StreamingDefaultTransfer
// Store message in memory stl queue of single process for prototype
class StreamingDefaultTransfer : public StreamingTransfer{
 public:
  StreamingDefaultTransfer() : StreamingTransfer() {}
  ~StreamingDefaultTransfer() {
    DestoryTransfer();
  }
 protected:
  StreamingStatus InitProducer(StreamingChannelConfig &channel_config) override {
    std::cout << "Init Default Transfer Producer" << std::endl;
    return StreamingStatus::OK;
  };
  StreamingStatus InitConsumer(StreamingChannelConfig &channel_config) override {
    std::cout << "Init Default Transfer Consumer" << std::endl;
    return StreamingStatus::OK;
  };
  StreamingStatus DestoryProducer() override {
    std::cout << "Destory Default Transfer Producer" << std::endl;
    return StreamingStatus::OK;
  }
  StreamingStatus DestoryConsumer() override {
    std::cout << "Destory Default Transfer Consumer" << std::endl;
    return StreamingStatus::OK;
  }
  StreamingStatus ProduceMessage(StreamingChannelInfo &channel_info, std::shared_ptr<StreamingMessage> msg) override {
    message_store_.push(msg);
    std::cout << "Produce Message" << std::endl;
    return StreamingStatus::OK;
  }

  StreamingStatus ConsumeMessage(StreamingChannelInfo &channel_info, std::shared_ptr<StreamingMessage> &msg) override {
    std::cout << "Consume Message" << std::endl;
    if (!message_store_.empty()) {
      msg = message_store_.front();
      message_store_.pop();
    }
    return StreamingStatus::OK;
  };

 private:
  static std::queue<std::shared_ptr<StreamingMessage>> message_store_;

};
#endif //STREAMING_PROTOTYPE_STREAMING_TRANSFORMATION_H
