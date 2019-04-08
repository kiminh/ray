#ifndef STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H
#define STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H

#include <string>
#include <cstring>

#include "ray/id.h"

namespace ray {
namespace streaming {
typedef ray::UniqueID StreamingChannelIndex;

class StreamingChannelInfo {
 private:
  uint64_t seq_id_;
  uint64_t message_id_;
  StreamingChannelIndex index_;

};
}
}

#endif // STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H
