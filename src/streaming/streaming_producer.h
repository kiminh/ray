//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_PRODUCER_H
#define RAY_STREAMING_STREAMING_PRODUCER_H

#include "streaming_channel.h"
#include "streaming_message.h"
#include "streaming_transfer.h"
namespace ray {
namespace streaming {
class StreamingProducer : public StreamingChannel {
 public:
  StreamingProducer(std::shared_ptr<StreamingChannelConfig> channel_config,
                    std::shared_ptr<StreamingTransfer> transfer)
    : StreamingChannel(channel_config, transfer) {}

  StreamingStatus InitChannel() override {
    transfer_->InitTransfer(channel_config_.operator*(), StreamingTransferRole::StreamingProduer);
    return StreamingStatus::OK;
  }

  StreamingStatus DestoryChannel() override {
    transfer_->DestoryTransfer();
    return StreamingStatus::OK;
  }
// Writer -> Strategy Function(Transfer handler）
// Reader -> Strategy Function(Transfer handler）
  StreamingStatus ProduceMessage(const StreamingChannelIndex &index, std::shared_ptr<StreamingMessage> msg) {
    return strategy_implementor_->ProduceMessage(channel_map_[index],
      std::bind(&StreamingTransfer::ProduceMessage, transfer_, channel_map_[index], msg));
  }

};
}
}

#endif //RAY_STREAMING_STREAMING_PRODUCER_H
