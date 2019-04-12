//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_STRATEGY_IMPLEMENTOR_H
#define RAY_STREAMING_STREAMING_STRATEGY_IMPLEMENTOR_H

#include "streaming_constant.h"
#include "streaming_channel_meta.h"

#include <functional>
#include <iostream>

namespace ray {
namespace streaming {

class StreamingChannelInfo;

class StreamingStrategyImplementor {
 public:
  typedef std::function<void()> ProduceHandler;
  virtual StreamingStatus ProduceMessage(StreamingChannelInfo &channel_info, ProduceHandler handler) = 0;
  virtual StreamingStatus ConsumeMessage(StreamingChannelInfo &channel_info, ProduceHandler handler) = 0;
  virtual ~StreamingStrategyImplementor() {};
};

class StreamingDefaultStrategyImplementor : public StreamingStrategyImplementor {

 public:
  StreamingStatus ProduceMessage(StreamingChannelInfo &channel_info, ProduceHandler handler) override;

  StreamingStatus ConsumeMessage(StreamingChannelInfo &channel_info, ProduceHandler handler) override;

  ~StreamingDefaultStrategyImplementor() {}

};
}
}

#endif //RAY_STREAMING_STREAMING_STRATEGY_IMPLEMENTOR_H
