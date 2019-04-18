#include "streaming_strategy_implementor.h"
#include "ray/util/logging.h"
namespace ray {
namespace streaming {
StreamingStatus StreamingDefaultStrategyImplementor::ProduceMessage(
    StreamingChannelInfo &channel_info, ProduceHandler handler) {
  RAY_LOG(DEBUG) << "before handle produce message";
  handler();
  RAY_LOG(DEBUG) << "after handle produce message";
  return StreamingStatus::OK;
};

StreamingStatus StreamingDefaultStrategyImplementor::ConsumeMessage(
    StreamingChannelInfo &channel_info, ProduceHandler handler) {
  RAY_LOG(DEBUG) << "before handle consume message";
  handler();
  RAY_LOG(DEBUG) << "after handle consume message";
  return StreamingStatus::OK;
};
}  // namespace streaming
}  // namespace ray
