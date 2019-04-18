//
// Created by ashione on 2019/4/1.
//

#include "streaming_config.h"
namespace ray {
namespace streaming {

std::vector<StreamingChannelIndex> &StreamingChannelConfig::GetIndexes() {
  return indexes_;
}

StreamingDefaultChannelConfig::StreamingDefaultChannelConfig(
    std::vector<StreamingChannelIndex> &indexes) {
  indexes_ = indexes;
}

}  // namespace streaming

}  // namespace ray
