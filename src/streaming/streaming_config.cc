//
// Created by ashione on 2019/4/1.
//

#include "streaming_config.h"
namespace ray {
namespace streaming {

StreamingChannelConfig::StreamingChannelConfig(std::vector<StreamingChannelId> &channel_id_vec)
  : channel_transfer_id_vec_(channel_id_vec) {}
std::vector<StreamingChannelId> &StreamingChannelConfig::GetTransferIdVec() {
  return channel_transfer_id_vec_;
};

std::vector<StreamingChannelId> &StreamingTransferConfig::GetIndexes() {
  return indexes_;
}

StreamingDefaultTransferConfig::StreamingDefaultTransferConfig(
  std::vector<StreamingChannelId> &indexes) {
  indexes_ = indexes;
}
}  // namespace streaming

}  // namespace ray
