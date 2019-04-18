//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_CHANNEL_H
#define RAY_STREAMING_STREAMING_CHANNEL_H
#include <cstring>
#include <string>
#include <unordered_map>

#include "streaming_config.h"
#include "streaming_constant.h"
#include "streaming_metrics.h"
#include "streaming_strategy_implementor.h"
#include "streaming_transfer.h"

namespace ray {
namespace streaming {
class StreamingTransfer;

class StreamingChannel {
 public:
  StreamingChannel(std::shared_ptr<StreamingChannelConfig> channel_config,
                   std::shared_ptr<StreamingTransfer> transfer)
      : channel_config_(channel_config), transfer_(transfer) {
    strategy_implementor_.reset(new StreamingDefaultStrategyImplementor());
  }

  virtual StreamingStatus InitChannel() = 0;
  virtual StreamingStatus DestoryChannel() = 0;
  virtual ~StreamingChannel(){};

 protected:
  std::unordered_map<StreamingChannelId, StreamingChannelInfo> channel_map_;
  std::shared_ptr<StreamingChannelConfig> channel_config_;
  std::shared_ptr<StreamingTransfer> transfer_;
  std::shared_ptr<StreamingMetricsReporter> metrics_reporter_;
  std::shared_ptr<StreamingStrategyImplementor> strategy_implementor_;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_STREAMING_CHANNEL_H
