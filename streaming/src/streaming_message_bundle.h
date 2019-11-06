#ifndef RAY_STREAMING_MESSAGE_BUNDLE_H
#define RAY_STREAMING_MESSAGE_BUNDLE_H

#include <ctime>
#include <list>
#include <numeric>

#include "streaming_message.h"
#include "streaming_serializable.h"

namespace ray {
namespace streaming {

enum class StreamingMessageBundleType : uint32_t {
  Empty = 1,
  Barrier = 2,
  Bundle = 3,
  MIN = Empty,
  MAX = Bundle
};

class StreamingMessageBundleMeta;
class StreamingMessageBundle;

typedef std::shared_ptr<StreamingMessageBundle> StreamingMessageBundlePtr;
typedef std::shared_ptr<StreamingMessageBundleMeta> StreamingMessageBundleMetaPtr;

/*
        +--------------------+
        | MagicNum=U32       |
        +--------------------+
        | BundleTs=U64       |
        +--------------------+
        | LastMessageId=U64  |
        +--------------------+
        | MessageListSize=U32|
        +--------------------+
        | BundleType=U32     |
        +--------------------+
        | RawBundleSize=U32  |
        +--------------------+
        | RawData=var(N*Msg) |
        +--------------------+
*/

constexpr uint32_t kMessageBundleMetaHeaderSize = sizeof(uint32_t) + sizeof(uint32_t) +
                                                  sizeof(uint64_t) + sizeof(uint64_t) +
                                                  sizeof(StreamingMessageBundleType);

constexpr uint32_t kMessageBundleHeaderSize =
    kMessageBundleMetaHeaderSize + sizeof(uint32_t);

class StreamingMessageBundleMeta : public StreamingSerializable {
 public:
  static const uint32_t StreamingMessageBundleMagicNum = 0xCAFEBABA;

 protected:
  uint64_t message_bundle_ts_;

  uint64_t last_message_id_;

  uint32_t message_list_size_;

  StreamingMessageBundleType bundle_type_;

 public:
  explicit StreamingMessageBundleMeta(uint64_t, uint64_t, uint32_t,
                                      StreamingMessageBundleType);

  explicit StreamingMessageBundleMeta(StreamingMessageBundleMeta *);

  explicit StreamingMessageBundleMeta();

  virtual ~StreamingMessageBundleMeta(){};

  bool operator==(StreamingMessageBundleMeta &) const;

  bool operator==(StreamingMessageBundleMeta *) const;

  inline uint64_t GetMessageBundleTs() const { return message_bundle_ts_; }

  inline uint64_t GetLastMessageId() const { return last_message_id_; }

  inline uint32_t GetMessageListSize() const { return message_list_size_; }

  inline StreamingMessageBundleType GetBundleType() const { return bundle_type_; }

  inline bool IsBarrier() { return StreamingMessageBundleType::Barrier == bundle_type_; }
  inline bool IsBundle() { return StreamingMessageBundleType::Bundle == bundle_type_; }

  STREAMING_SERIALIZATION
  STREAMING_DESERIALIZATION(StreamingMessageBundleMetaPtr)
  STREAMING_SERIALIZATION_LENGTH { return kMessageBundleMetaHeaderSize; };

  std::string ToString() {
    return std::to_string(last_message_id_) + "," + std::to_string(message_list_size_) +
           "," + std::to_string(message_bundle_ts_) + "," +
           std::to_string(static_cast<uint32_t>(bundle_type_));
  }
};

class StreamingMessageBundle : public StreamingMessageBundleMeta {
 private:
  uint32_t raw_bundle_size_;

  // Lazy serlization/deserlization.
  std::list<StreamingMessagePtr> message_list_;

 public:
  explicit StreamingMessageBundle(std::list<StreamingMessagePtr> &&, uint64_t, uint64_t,
                                  StreamingMessageBundleType, uint32_t raw_data_size = 0);

  // Duplicated copy if left reference in constructor.
  explicit StreamingMessageBundle(std::list<StreamingMessagePtr> &, uint64_t, uint64_t,
                                  StreamingMessageBundleType, uint32_t raw_data_size = 0);

  explicit StreamingMessageBundle(uint64_t, uint64_t);

  explicit StreamingMessageBundle(StreamingMessageBundle &);

  virtual ~StreamingMessageBundle() = default;

  inline uint32_t GetRawBundleSize() const { return raw_bundle_size_; }

  bool operator==(StreamingMessageBundle &) const;

  bool operator==(StreamingMessageBundle *) const;

  void GetMessageList(std::list<StreamingMessagePtr> &);

  const std::list<StreamingMessagePtr> &GetMessageList() const { return message_list_; }

  STREAMING_SERIALIZATION
  STREAMING_DESERIALIZATION(StreamingMessageBundlePtr)

  STREAMING_SERIALIZATION_LENGTH { return kMessageBundleHeaderSize + raw_bundle_size_; };

  static void GetMessageListFromRawData(const uint8_t *, uint32_t, uint32_t,
                                        std::list<StreamingMessagePtr> &);
  static void ConvertMessageListToRawData(const std::list<StreamingMessagePtr> &,
                                          uint32_t, uint8_t *);
};
}  // namespace streaming
}  // namespace ray

#endif  // RAY_STREAMING_MESSAGE_BUNDLE_H
