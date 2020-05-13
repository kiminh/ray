#ifndef RAY_COMMON_TASK_TASK_COMMON_H
#define RAY_COMMON_TASK_TASK_COMMON_H

#include "ray/protobuf/common.pb.h"
#include "ray/raylet/format/task_spec_generated.h"

namespace ray {

// NOTE(hchen): Below we alias `ray::rpc::Language|TaskType)` in  `ray` namespace.
// The reason is because other code should use them as if they were defined in this
// `task_common.h` file, shouldn't care about the implementation detail that they
// are defined in protobuf.

/// See `common.proto` for definition of `Language` enum.
using Language = rpc::Language;
/// See `common.proto` for definition of `TaskType` enum.
using TaskType = rpc::TaskType;

/// A helper to cast flatbuf::Language to proto::Language.
inline Language FromFlatbufLanguage(rpc::flatbuf::Language language) {
  /// Note that the reason for casting here explicitly is to avoid
  /// introducing mistake by implicitly casting.
  if (language == rpc::flatbuf::Language::PYTHON) {
    return Language::PYTHON;
  } else if (language == rpc::flatbuf::Language::JAVA) {
    return Language::JAVA;
  } else {
    return Language::CPP;
  }
}

/// A helper to cast proto::Language to flatbuf::Language.
inline rpc::flatbuf::Language ToFlatbufLanguage(Language language) {
  if (language == Language::PYTHON) {
    return rpc::flatbuf::Language::PYTHON;
  } else if (language == Language::JAVA) {
    return rpc::flatbuf::Language::JAVA;
  } else {
    return rpc::flatbuf::Language::CPP;
  }
}

}  // namespace ray

#endif
