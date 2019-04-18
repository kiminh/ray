//
// Created by ashione on 2019/4/1.
//

#include "streaming_consumer.h"
namespace ray {
namespace streaming {

StreamingConsumer::StreamingConsumer(
    std::shared_ptr<StreamingChannelConfig> channel_config,
    std::shared_ptr<StreamingConsumeTransfer> transfer)
    : StreamingChannel(channel_config, transfer) {}

StreamingStatus StreamingConsumer::InitChannel() {
  transfer_->InitTransfer();
  return StreamingStatus::OK;
}

StreamingStatus StreamingConsumer::DestoryChannel() {
  transfer_->DestoryTransfer();
  return StreamingStatus::OK;
}

StreamingStatus StreamingConsumer::ConsumeMessage(
    std::shared_ptr<StreamingMessage> &msg) {
  // return transfer_->ConsumeMessage(msg);
  StreamingChannelInfo fake_info;
  return strategy_implementor_->ConsumeMessage(
      fake_info, std::bind(&StreamingConsumeTransfer::ConsumeMessage,
                           dynamic_cast<StreamingConsumeTransfer *>(transfer_.get()),
                           std::ref(fake_info), std::ref(msg)));
  // Update channel info by fake info
}
}  // namespace streaming
}  // namespace ray
