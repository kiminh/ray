#include "ray/util/logging.h"
#include "streaming_channel.h"
#include "streaming_transfer.h"
#include "streaming_producer.h"
#include "streaming_consumer.h"

#include <iostream>
#include <thread>
using namespace ray::streaming;

void testWriteRead() {
StreamingChannelIndex index;
  RAY_LOG(INFO) << "channel " << index;

  std::shared_ptr<StreamingChannelConfig> channel_config(new StreamingDefaultChannelConfig);
  std::shared_ptr<StreamingProduceTransfer> producer_transfer(new StreamingDefaultProduceTransfer);
  std::shared_ptr<StreamingConsumeTransfer> consumer_transfer(new StreamingDefaultConsumeTransfer);

  std::shared_ptr<StreamingProducer> producer(new StreamingProducer(channel_config, producer_transfer));
  producer->InitChannel();
  std::shared_ptr<StreamingConsumer> consumer(new StreamingConsumer(channel_config, consumer_transfer));
  consumer->InitChannel();
  uint8_t data[5] = {1,2,3,4,5};
  std::shared_ptr<StreamingMessage> msg(new StreamingMessage(data, 5));
  producer->ProduceMessage(index, msg);
  std::shared_ptr<StreamingMessage> recevied_msg;
  consumer->ConsumeMessage(recevied_msg);
  assert(std::memcmp(recevied_msg->GetData(), data, 5) == 0);
}

void testMultiWriteRead() {
  StreamingChannelIndex index;

  std::shared_ptr<StreamingChannelConfig> channel_config(new StreamingDefaultChannelConfig);
  std::shared_ptr<StreamingProduceTransfer> producer_transfer(new StreamingDefaultProduceTransfer);
  std::shared_ptr<StreamingConsumeTransfer> consumer_transfer(new StreamingDefaultConsumeTransfer);

  std::shared_ptr<StreamingProducer> producer(new StreamingProducer(channel_config, producer_transfer));
  producer->InitChannel();
  std::shared_ptr<StreamingConsumer> consumer1(new StreamingConsumer(channel_config, consumer_transfer));
  consumer1->InitChannel();

  std::shared_ptr<StreamingConsumer> consumer2(new StreamingConsumer(channel_config, consumer_transfer));
  consumer2->InitChannel();

  std::thread reader_thread1([consumer1]() {
    for(int i= 0; i< 500; ++i) {
      std::shared_ptr<StreamingMessage> recevied_msg;
      while (!recevied_msg) {
        consumer1->ConsumeMessage(std::ref(recevied_msg));
      }
      assert(std::memcmp(recevied_msg->GetData(), reinterpret_cast<const uint8_t *>(&i), sizeof(int)) == 0);
      int result = *reinterpret_cast<const int*>(recevied_msg->GetData());
      RAY_LOG(INFO) << "[Reader1] recevied data " << result;
    }
  });
  std::thread reader_thread2([consumer2]() {
    for(int i= 0; i< 500; ++i) {
      std::shared_ptr<StreamingMessage> recevied_msg;
      while (!recevied_msg) {
        consumer2->ConsumeMessage(std::ref(recevied_msg));
      }
      assert(std::memcmp(recevied_msg->GetData(), reinterpret_cast<const uint8_t *>(&i), sizeof(int)) == 0);
      int result = *reinterpret_cast<const int*>(recevied_msg->GetData());
      RAY_LOG(INFO) << "[Reader2] recevied data " << result;
    }
  });
  std::thread writer_thread([producer, index]() {
    for(int i = 0; i < 1000; ++i)  {
      std::shared_ptr<StreamingMessage> msg(new StreamingMessage(reinterpret_cast<const uint8_t *>(&i), sizeof(int)));
      producer->ProduceMessage(index, msg);
    }
  });

  writer_thread.join();
  reader_thread1.join();
  reader_thread2.join();
}

int main() {
  testWriteRead();
  testMultiWriteRead();
  return 0;
}
