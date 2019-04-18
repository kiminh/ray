#ifndef STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H
#define STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H

#include <cstring>
#include <string>

#include "ray/id.h"

namespace ray {
namespace streaming {
typedef ray::UniqueID StreamingChannelIndex;

class StreamingChannelInfo {
 public:
  StreamingChannelIndex &Index() { return index_; }
  StreamingChannelInfo(StreamingChannelIndex &index) : index_(index){};
  StreamingChannelInfo(){};

 private:
  StreamingChannelIndex index_;
};
}  // namespace streaming
}  // namespace ray

#endif  // STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H
