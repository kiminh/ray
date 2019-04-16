#include "ray/util/logging.h"
#include "streaming_channel.h"
#include "streaming_transfer.h"
#include "streaming_producer.h"
#include "streaming_consumer.h"

#include "gtest/gtest.h"

#include <iostream>
#include <thread>
#include <vector>
#include <algorithm>

using namespace ray::streaming;

TEST(ChannelTest, testWriterReader) {
  StreamingChannelIndex index;
  RAY_LOG(INFO) << "channel " << index;
  std::vector<StreamingChannelIndex> indexes;
  indexes.push_back(index);

  std::shared_ptr<StreamingChannelConfig> channel_config(new StreamingDefaultChannelConfig(indexes));
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
  ASSERT_EQ(std::memcmp(recevied_msg->GetData(), data, 5), 0);
}

TEST(ChannelTest, testMultiWriteRead) {
  StreamingChannelIndex index;
  std::vector<StreamingChannelIndex> indexes;
  indexes.push_back(index);

  std::shared_ptr<StreamingChannelConfig> channel_config(new StreamingDefaultChannelConfig(indexes));
  std::shared_ptr<StreamingProduceTransfer> producer_transfer(new StreamingDefaultProduceTransfer);
  std::shared_ptr<StreamingConsumeTransfer> consumer_transfer(new StreamingDefaultConsumeTransfer);

  std::shared_ptr<StreamingProducer> producer(new StreamingProducer(channel_config, producer_transfer));
  producer->InitChannel();
  std::shared_ptr<StreamingConsumer> consumer1(new StreamingConsumer(channel_config, consumer_transfer));
  consumer1->InitChannel();

  std::shared_ptr<StreamingConsumer> consumer2(new StreamingConsumer(channel_config, consumer_transfer));
  consumer2->InitChannel();
  std::vector<int> result1, result2;

  std::thread reader_thread1([consumer1, &result1]() {
    for(int i= 0; i< 500; ++i) {
      std::shared_ptr<StreamingMessage> recevied_msg;
      while (!recevied_msg) {
        consumer1->ConsumeMessage(std::ref(recevied_msg));
      }
      int result = *reinterpret_cast<const int*>(recevied_msg->GetData());
      result1.push_back(result);
      RAY_LOG(INFO) << "[Reader1] recevied data " << result;
    }
  });
  std::thread reader_thread2([consumer2, &result2]() {
    for(int i= 0; i< 500; ++i) {
      std::shared_ptr<StreamingMessage> recevied_msg;
      while (!recevied_msg) {
        consumer2->ConsumeMessage(std::ref(recevied_msg));
      }
      int result = *reinterpret_cast<const int*>(recevied_msg->GetData());
      result2.push_back(result);
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
  std::copy(result2.begin(), result2.end(), std::back_inserter(result1));
  std::sort(result1.begin(), result1.end());
  std::for_each(result1.begin(), result1.end(), [](int n) {
    static int start_n = 0;
    EXPECT_EQ(start_n++ , n);
  });

}

TEST(ChannelTest, testIndexes) {
  StreamingChannelIndex index1 = StreamingChannelIndex::from_random();
  StreamingChannelIndex index2 = StreamingChannelIndex::from_random();
  std::vector<StreamingChannelIndex> indexes;
  indexes.push_back(index1);
  indexes.push_back(index2);

  std::vector<StreamingChannelIndex> r1_indexes;
  r1_indexes.push_back(index1);
  std::vector<StreamingChannelIndex> r2_indexes;
  r2_indexes.push_back(index2);

  std::shared_ptr<StreamingChannelConfig> channel_config(new StreamingDefaultChannelConfig(indexes));
  std::shared_ptr<StreamingChannelConfig> r1_channel_config(
    new StreamingDefaultChannelConfig(r1_indexes));

  std::shared_ptr<StreamingChannelConfig> r2_channel_config(
    new StreamingDefaultChannelConfig(r2_indexes));

  std::shared_ptr<StreamingProduceTransfer> producer_transfer(new StreamingDefaultProduceTransfer);
  std::shared_ptr<StreamingConsumeTransfer> consumer_transfer1(new StreamingDefaultConsumeTransfer);
  std::shared_ptr<StreamingConsumeTransfer> consumer_transfer2(new StreamingDefaultConsumeTransfer);

  std::shared_ptr<StreamingProducer> producer(new StreamingProducer(channel_config, producer_transfer));
  producer->InitChannel();
  std::shared_ptr<StreamingConsumer> consumer1(new StreamingConsumer(r1_channel_config, consumer_transfer1));
  consumer1->InitChannel();

  std::shared_ptr<StreamingConsumer> consumer2(new StreamingConsumer(r2_channel_config, consumer_transfer2));
  consumer2->InitChannel();
  std::vector<int> result1, result2;

  std::thread reader_thread1([consumer1, &result1]() {
    for(int i= 0; i< 500; ++i) {
      std::shared_ptr<StreamingMessage> recevied_msg;
      while (!recevied_msg) {
        consumer1->ConsumeMessage(std::ref(recevied_msg));
      }
      int result = *reinterpret_cast<const int*>(recevied_msg->GetData());
      result1.push_back(result);
      RAY_LOG(INFO) << "[Reader1] recevied data " << result << " result len " << result1.size();
    }
  });
  std::thread reader_thread2([consumer2, &result2]() {
    for(int i= 0; i< 500; ++i) {
      std::shared_ptr<StreamingMessage> recevied_msg;
      while (!recevied_msg) {
        consumer2->ConsumeMessage(std::ref(recevied_msg));
      }
      int result = *reinterpret_cast<const int*>(recevied_msg->GetData());
      result2.push_back(result);
      RAY_LOG(INFO) << "[Reader2] recevied data " << result << " result len " << result2.size();
    }
  });
  std::thread writer_thread([producer, &indexes]() {
    for(int i = 0; i < 1000; ++i)  {
      std::shared_ptr<StreamingMessage> msg(new StreamingMessage(reinterpret_cast<const uint8_t *>(&i), sizeof(int)));
      producer->ProduceMessage(indexes[i % 2], msg);
    }
  });
  writer_thread.join();
  reader_thread1.join();
  reader_thread2.join();

  std::for_each(result1.begin(), result1.end(), [](int n) {
    static int start_n = 0;
    EXPECT_EQ(start_n, n);
    start_n += 2;
  });

  std::for_each(result2.begin(), result2.end(), [](int n) {
    static int start_n = 1;
    EXPECT_EQ(start_n, n);
    start_n += 2;
  });

}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
