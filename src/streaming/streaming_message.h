//
// Created by ashione on 2019/4/1.
//

#ifndef RAY_STREAMING_STREAMING_MESSAGE_H
#define RAY_STREAMING_STREAMING_MESSAGE_H
#include <string>
namespace ray {
namespace streaming {
class StreamingMessage {
 public:
  StreamingMessage(const uint8_t *data, uint32_t data_len);
  StreamingMessage() {}

  const uint8_t *GetData() const { return data_.get(); }
  const uint32_t Size() const { return data_len_; }
 private:
  std::unique_ptr<uint8_t> data_;
  uint32_t data_len_;
};
}
}

#endif //RAY_STREAMING_STREAMING_MESSAGE_H
