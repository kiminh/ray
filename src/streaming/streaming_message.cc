//
// Created by ashione on 2019/4/1.
//

#include "streaming_message.h"
namespace ray {
namespace streaming {
StreamingMessage::StreamingMessage(const uint8_t *data, uint32_t data_len)
    : data_len_(data_len) {
  data_.reset(new uint8_t[data_len]);
  std::memcpy(data_.get(), data, data_len);
}
}  // namespace streaming
}  // namespace ray