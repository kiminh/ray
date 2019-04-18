#ifndef RAY_STREAMING_STREAMING_CONFIG_H
#define RAY_STREAMING_STREAMING_CONFIG_H
#include <vector>

#include "streaming_channel_meta.h"

namespace ray {
namespace streaming {
class StreamingConfig {};

class StreamingMetricConfig {};

class StreamingChannelConfig {
 public:
  std::vector<StreamingChannelIndex> &GetIndexes();

 protected:
  std::vector<StreamingChannelIndex> indexes_;
};

class StreamingDefaultChannelConfig : public StreamingChannelConfig {
 public:
  StreamingDefaultChannelConfig(std::vector<StreamingChannelIndex> &indexes);
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_STREAMING_CONFIG_H
