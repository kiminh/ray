#ifndef RAY_STREAMING_STREAMING_CONFIG_H
#define RAY_STREAMING_STREAMING_CONFIG_H
#include <vector>

#include "ray/id.h"

namespace ray {
namespace streaming {

typedef ray::UniqueID StreamingChannelId;

class StreamingChannelConfig {
 public:
  StreamingChannelConfig(std::vector<StreamingChannelId> &channel_id_vec);
  std::vector<StreamingChannelId> &GetTransferIdVec();
 private:
  std::vector<StreamingChannelId> channel_transfer_id_vec_;
};

class StreamingMetricConfig {};

class StreamingTransferConfig {
 public:
  std::vector<StreamingChannelId> &GetIndexes();

 protected:
  std::vector<StreamingChannelId> indexes_;
};

class StreamingDefaultTransferConfig : public StreamingTransferConfig {
 public:
  StreamingDefaultTransferConfig(std::vector<StreamingChannelId> &indexes);
};
class StreamingChannelInfo {
 public:
  StreamingChannelId &Index() { return index_; }
  StreamingChannelInfo(StreamingChannelId &index) : index_(index){};
  StreamingChannelInfo(){};

 private:
  StreamingChannelId index_;
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_STREAMING_CONFIG_H
