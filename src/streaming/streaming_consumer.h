//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_CONSUMER_H
#define RAY_STREAMING_STREAMING_CONSUMER_H
#include "streaming_channel.h"
#include "streaming_transfer.h"
namespace ray {
namespace streaming {
class StreamingConsumer : public StreamingChannel {
 public:
  StreamingConsumer(std::shared_ptr<StreamingChannelConfig> channel_config,
                    std::shared_ptr<StreamingConsumeTransfer> transfer);

  StreamingStatus InitChannel() override;

  StreamingStatus DestoryChannel() override;

  StreamingStatus ConsumeMessage(std::shared_ptr<StreamingMessage> &msg);
};
}  // namespace streaming
}  // namespace ray
#endif  // RAY_STREAMING_STREAMING_CONSUMER_H
