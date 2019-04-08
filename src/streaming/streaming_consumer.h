//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_CONSUMER_H
#define RAY_STREAMING_STREAMING_CONSUMER_H
#include "streaming_channel.h"
#include "streaming_transfer.h"
namespace ray {
namespace streaming {
class StreamingConsumer : public StreamingChannel{
 public:
  StreamingConsumer(std::shared_ptr<StreamingChannelConfig> channel_config,
                    std::shared_ptr<StreamingTransfer> transfer)
    : StreamingChannel(channel_config, transfer) {}

  StreamingStatus InitChannel() override {
    transfer_->InitTransfer(channel_config_.operator*(), StreamingTransferRole::StreamingConsumer);
    return StreamingStatus::OK;
  }

  StreamingStatus DestoryChannel() override{
    transfer_->DestoryTransfer();
    return StreamingStatus::OK;
  }

  StreamingStatus ConsumeMessage(std::shared_ptr<StreamingMessage> &msg) {
    // return transfer_->ConsumeMessage(msg);
    StreamingChannelInfo fake_info;
    return strategy_implementor_->ConsumeMessage(fake_info,
      std::bind(&StreamingTransfer::ConsumeMessage, transfer_, std::ref(fake_info), std::ref(msg)));
    // Update channel info by fake info
  }
};
}
}
#endif //RAY_STREAMING_STREAMING_CONSUMER_H
