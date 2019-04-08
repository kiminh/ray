#ifndef STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H
#define STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H

#include <string>
#include <cstring>
// Ray UniqueId
// index实际用的是ray uniqueID 对象，这里用index来作为模拟代替
class StreamingChannelIndex {
 public:
  StreamingChannelIndex(uint8_t *data) {
    std::memcpy(data_, data, 20);
  }
  StreamingChannelIndex() {
    std::memset(data_, 0, 20);
  }

  bool operator==(const StreamingChannelIndex& channel_index) const {
    return std::memcmp(data_, channel_index.GetData(), 20) == 0;

  }

  bool operator!=(const StreamingChannelIndex& channel_index) const {
    return !(*this == channel_index);

  }
  // Just example for hash function
  size_t hash() const {
    return data_[0] + data_[1];
  }

  const uint8_t *GetData() const {
    return data_;
  }

 private:
  uint8_t data_[20];
};

namespace std {
template<>
struct hash<::StreamingChannelIndex> {
  size_t operator()(const ::StreamingChannelIndex &id) const { return id.hash(); }
};

template<>
struct hash<const ::StreamingChannelIndex> {
  size_t operator()(const ::StreamingChannelIndex &id) const { return id.hash(); }
};
}

class StreamingChannelInfo {
 private:
  uint64_t seq_id_;
  uint64_t message_id_;
  StreamingChannelIndex index_;

};

#endif // STREAMING_PROTOTYPE_STREAMING_CHANNEL_META_H
