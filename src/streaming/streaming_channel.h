//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_CHANNEL_H
#define RAY_STREAMING_STREAMING_CHANNEL_H
#include <unordered_map>
#include <string>
#include <cstring>

#include "streaming_config.h"
#include "streaming_channel_meta.h"
#include "streaming_constant.h"
#include "streaming_transfer.h"
#include "streaming_metrics.h"
#include "streaming_strategy_implementor.h"

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
  virtual ~StreamingChannel() {};

 protected:
  std::unordered_map<StreamingChannelIndex, StreamingChannelInfo> channel_map_;
  std::shared_ptr<StreamingChannelConfig> channel_config_;
  std::shared_ptr<StreamingTransfer> transfer_;
  std::shared_ptr<StreamingMetricsReporter> metrics_reporter_;
  std::shared_ptr<StreamingStrategyImplementor> strategy_implementor_;
};
}
}

#endif //RAY_STREAMING_STREAMING_CHANNEL_H
