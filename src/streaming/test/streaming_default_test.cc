#include "streaming_channel.h"
#include "streaming_transfer.h"
#include "streaming_producer.h"
#include "streaming_consumer.h"

#include <iostream>
using namespace ray::streaming;

void hello() {
  std::cout << "Hello, World!" << std::endl;
}

int main() {
  hello();
  StreamingChannelIndex index;
  std::cout << "channel " << index << std::endl;

  std::shared_ptr<StreamingChannelConfig> channel_config(new StreamingDefaultChannelConfig);
  std::shared_ptr<StreamingTransfer> producer_transfer(new StreamingDefaultTransfer);
  std::shared_ptr<StreamingTransfer> consumer_transfer(new StreamingDefaultTransfer);

  std::shared_ptr<StreamingProducer> producer(new StreamingProducer(channel_config, producer_transfer));
  producer->InitChannel();
  std::shared_ptr<StreamingConsumer> consumer(new StreamingConsumer(channel_config, consumer_transfer));
  consumer->InitChannel();
  uint8_t data[5] = {1,2,3,4,5};
  std::shared_ptr<StreamingMessage> msg(new StreamingMessage(data, 5)) ;
  producer->ProduceMessage(index, msg);
  std::shared_ptr<StreamingMessage> recevied_msg;
  consumer->ConsumeMessage(recevied_msg);
  assert(std::memcmp(recevied_msg->GetData(), data, 5) == 0);
  return 0;
}
