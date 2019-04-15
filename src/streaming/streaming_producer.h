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
                    std::shared_ptr<StreamingProduceTransfer> transfer);
  StreamingStatus InitChannel() override;
  StreamingStatus DestoryChannel() override;
  StreamingStatus ProduceMessage(const StreamingChannelIndex &index, std::shared_ptr<StreamingMessage> msg);

};
}
}

#endif //RAY_STREAMING_STREAMING_PRODUCER_H
