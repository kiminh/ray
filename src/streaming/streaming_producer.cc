//
// Created by ashione on 2019/4/1.
//

#include "streaming_producer.h"
namespace ray {
namespace streaming {

StreamingProducer::StreamingProducer(std::shared_ptr<StreamingChannelConfig> channel_config,
                  std::shared_ptr<StreamingProduceTransfer> transfer)
  : StreamingChannel(channel_config, transfer) {}

StreamingStatus StreamingProducer::InitChannel() {
  transfer_->InitTransfer(channel_config_.operator*());
  return StreamingStatus::OK;
}

StreamingStatus StreamingProducer::DestoryChannel() {
  transfer_->DestoryTransfer();
  return StreamingStatus::OK;
}

// Writer -> Strategy Function(Transfer handler
// Reader -> Strategy Function(Transfer handler）
StreamingStatus StreamingProducer::ProduceMessage(
  const StreamingChannelIndex &index,
  std::shared_ptr<StreamingMessage> msg) {
  return strategy_implementor_->ProduceMessage(channel_map_[index],
                                               std::bind(&StreamingProduceTransfer::ProduceMessage,
                                                         dynamic_cast<StreamingProduceTransfer*>(transfer_.get()),
                                                         channel_map_[index], msg));
}

}
}
