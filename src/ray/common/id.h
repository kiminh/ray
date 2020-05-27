// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RAY_ID_H_
#define RAY_ID_H_

#include <inttypes.h>
#include <limits.h>

#include <chrono>
#include <cstring>
#include <msgpack.hpp>
#include <mutex>
#include <random>
#include <string>

#include "plasma/common.h"
#include "ray/common/constants.h"
#include "ray/util/logging.h"
#include "ray/util/util.h"
#include "ray/util/visibility.h"

namespace ray {

class TaskID;
class WorkerID;
class UniqueID;
class JobID;

/// TODO(qwang): These 2 helper functions should be removed
/// once we separated the `WorkerID` from `UniqueID`.
///
/// A helper function that get the `DriverID` of the given job.
WorkerID ComputeDriverIdFromJob(const JobID &job_id);

/// The type of this object. `PUT_OBJECT` indicates this object
/// is generated through `ray.put` during the task's execution.
/// And `RETURN_OBJECT` indicates this object is the return value
/// of a task.
enum class ObjectType : uint8_t {
  PUT_OBJECT = 0x0,
  RETURN_OBJECT = 0x1,
};

using ObjectIDFlagsType = uint16_t;
using ObjectIDIndexType = uint32_t;

// Declaration.
uint64_t MurmurHash64A(const void *key, int len, unsigned int seed);

// Change the compiler alignment to 1 byte (default is 8).
#pragma pack(push, 1)

/// The `ID`s of Ray.
///
/// Please refer to the specification of Ray UniqueIDs.
/// https://github.com/ray-project/ray/blob/master/src/ray/design_docs/id_specification.md

template <typename T>
class BaseID {
 public:
  BaseID();
  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static T FromRandom();
  static T FromBinary(const std::string &binary);
  static const T &Nil();
  static size_t Size() { return T::Size(); }

  size_t Hash() const;
  bool IsNil() const;
  bool operator==(const BaseID &rhs) const;
  bool operator!=(const BaseID &rhs) const;
  const uint8_t *Data() const;
  std::string Binary() const;
  std::string Hex() const;

 protected:
  BaseID(const std::string &binary) {
    RAY_CHECK(binary.size() == Size() || binary.size() == 0)
        << "expected size is " << Size() << ", but got " << binary.size();
    std::memcpy(const_cast<uint8_t *>(this->Data()), binary.data(), binary.size());
  }
  // All IDs are immutable for hash evaluations. MutableData is only allow to use
  // in construction time, so this function is protected.
  uint8_t *MutableData();
  // For lazy evaluation, be careful to have one Id contained in another.
  // This hash code will be duplicated.
  mutable size_t hash_ = 0;
};

class UniqueID : public BaseID<UniqueID> {
 public:
  static size_t Size() { return kUniqueIDSize; }

  UniqueID() : BaseID() {}

  MSGPACK_DEFINE(id_);

 protected:
  UniqueID(const std::string &binary);

 protected:
  uint8_t id_[kUniqueIDSize];
};

class JobID : public BaseID<JobID> {
 public:
  static constexpr int64_t kLength = 2;

  static JobID FromInt(uint16_t value);

  static size_t Size() { return kLength; }

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static JobID FromRandom() = delete;

  JobID() : BaseID() {}

  MSGPACK_DEFINE(id_);

 private:
  uint8_t id_[kLength];
};

class ActorID : public BaseID<ActorID> {
 private:
  static constexpr size_t kUniqueBytesLength = 4;

 public:
  /// Length of `ActorID` in bytes.
  static constexpr size_t kLength = kUniqueBytesLength + JobID::kLength;

  /// Size of `ActorID` in bytes.
  ///
  /// \return Size of `ActorID` in bytes.
  static size_t Size() { return kLength; }

  /// Creates an `ActorID` by hashing the given information.
  ///
  /// \param job_id The job id to which this actor belongs.
  /// \param parent_task_id The id of the task which created this actor.
  /// \param parent_task_counter The counter of the parent task.
  ///
  /// \return The random `ActorID`.
  static ActorID Of(const JobID &job_id, const TaskID &parent_task_id,
                    const size_t parent_task_counter);

  /// Creates a nil ActorID with the given job.
  ///
  /// \param job_id The job id to which this actor belongs.
  ///
  /// \return The `ActorID` with unique bytes being nil.
  static ActorID NilFromJob(const JobID &job_id);

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static ActorID FromRandom() = delete;

  /// Constructor of `ActorID`.
  ActorID() : BaseID() {}

  /// Get the job id to which this actor belongs.
  ///
  /// \return The job id to which this actor belongs.
  JobID JobId() const;

  MSGPACK_DEFINE(id_);

 private:
  uint8_t id_[kLength];
};

class PlacementGroupID : public BaseID<PlacementGroupID> {
 private:
  static constexpr size_t kUniqueBytesLength = 4;

 public:
  /// Length of `PlacementGroupID` in bytes.
  static constexpr size_t kLength = kUniqueBytesLength + JobID::kLength;

  /// Size of `PlacementGroupID` in bytes.
  ///
  /// \return Size of `PlacementGroupID` in bytes.
  static size_t Size() { return kLength; }

  /// Creates a `PlacementGroupID` by hashing the given information.
  ///
  /// \param job_id The job id to which this placement group belongs.
  /// \param parent_task_id The id of the task which created this placement group.
  /// \param parent_task_counter The counter of the parent task.
  ///
  /// \return The random `ActorID`.
  static PlacementGroupID Of(const JobID &job_id, const TaskID &parent_task_id,
                    const size_t parent_task_counter);
    /// Constructor of `PlacementGroupID`.
  PlacementGroupID() : BaseID() {}

  /// Get the job id to which this placement group belongs.
  ///
  /// \return The job id to which this placement group belongs.
  JobID JobId() const;

  MSGPACK_DEFINE(id_);

private:
  uint8_t id_[kLength];

  // TODO(AlisaWu): fill the class of PlacementGroupID.


};

class TaskID : public BaseID<TaskID> {
 private:
  static constexpr size_t kUniqueBytesLength = 8;

 public:
  static constexpr size_t kLength = kUniqueBytesLength + ActorID::kLength;

  TaskID() : BaseID() {}

  static size_t Size() { return kLength; }

  static TaskID ComputeDriverTaskId(const WorkerID &driver_id);

  // Warning: this can duplicate IDs after a fork() call. We assume this never happens.
  static TaskID FromRandom() = delete;

  /// The ID generated for driver task.
  static TaskID ForDriverTask(const JobID &job_id);

  /// Generate driver task id for the given job.
  static TaskID ForFakeTask();

  /// Creates a TaskID for an actor creation task.
  ///
  /// \param actor_id The ID of the actor that will be created
  ///        by this actor creation task.
  ///
  /// \return The ID of the actor creation task.
  static TaskID ForActorCreationTask(const ActorID &actor_id);

  /// Creates a TaskID for actor task.
  ///
  /// \param job_id The ID of the job to which this task belongs.
  /// \param parent_task_id The ID of the parent task which submitted this task.
  /// \param parent_task_counter A count of the number of tasks submitted by the
  ///        parent task before this one.
  /// \param actor_id The ID of the actor to which this task belongs.
  ///
  /// \return The ID of the actor task.
  static TaskID ForActorTask(const JobID &job_id, const TaskID &parent_task_id,
                             size_t parent_task_counter, const ActorID &actor_id);

  /// Creates a TaskID for normal task.
  ///
  /// \param job_id The ID of the job to which this task belongs.
  /// \param parent_task_id The ID of the parent task which submitted this task.
  /// \param parent_task_counter A count of the number of tasks submitted by the
  ///        parent task before this one.
  ///
  /// \return The ID of the normal task.
  static TaskID ForNormalTask(const JobID &job_id, const TaskID &parent_task_id,
                              size_t parent_task_counter);

  /// Get the id of the actor to which this task belongs.
  ///
  /// \return The `ActorID` of the actor which creates this task.
  ActorID ActorId() const;

  /// Get the id of the job to which this task belongs.
  ///
  /// \return The `JobID` of the job which creates this task.
  JobID JobId() const;

  MSGPACK_DEFINE(id_);

 private:
  uint8_t id_[kLength];
};

class ObjectID : public BaseID<ObjectID> {
 private:
  static constexpr size_t kIndexBytesLength = sizeof(ObjectIDIndexType);

  static constexpr size_t kFlagsBytesLength = sizeof(ObjectIDFlagsType);

 public:
  /// The maximum number of objects that can be returned or put by a task.
  static constexpr int64_t kMaxObjectIndex = ((int64_t)1 << kObjectIdIndexSize) - 1;

  /// The length of ObjectID in bytes.
  static constexpr size_t kLength =
      kIndexBytesLength + kFlagsBytesLength + TaskID::kLength;

  ObjectID() : BaseID() {}

  /// The maximum index of object.
  ///
  /// It also means the max number of objects created (put or return) by one task.
  ///
  /// \return The maximum index of object.
  static uint64_t MaxObjectIndex() { return kMaxObjectIndex; }

  static size_t Size() { return kLength; }

  /// Generate ObjectID by the given binary string of a plasma id.
  ///
  /// \param from The binary string of the given plasma id.
  /// \return The ObjectID converted from a binary string of the plasma id.
  static ObjectID FromPlasmaIdBinary(const std::string &from);

  plasma::ObjectID ToPlasmaId() const;

  ObjectID(const plasma::UniqueID &from);

  /// Get the index of this object in the task that created it.
  ///
  /// \return The index of object creation according to the task that created
  /// this object.
  ObjectIDIndexType ObjectIndex() const;

  /// Compute the task ID of the task that created the object.
  ///
  /// \return The task ID of the task that created this object.
  TaskID TaskId() const;

  /// Whether this object is created by a task.
  ///
  /// \return True if this object is created by a task, otherwise false.
  bool CreatedByTask() const;

  /// Whether this object was created through `ray.put`.
  ///
  /// \return True if this object was created through `ray.put`.
  bool IsPutObject() const;

  /// Whether this object was created as a return object of a task.
  ///
  /// \return True if this object is a return value of a task.
  bool IsReturnObject() const;

  /// Compute the object ID of an object put by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param index What index of the object put in the task.
  ///
  /// \return The computed object ID.
  static ObjectID ForPut(const TaskID &task_id, ObjectIDIndexType put_index);

  /// Compute the object ID of an object returned by the task.
  ///
  /// \param task_id The task ID of the task that created the object.
  /// \param return_index What index of the object returned by in the task.
  ///
  /// \return The computed object ID.
  static ObjectID ForTaskReturn(const TaskID &task_id, ObjectIDIndexType return_index);

  /// Create an object id randomly.
  ///
  /// Warning: this can duplicate IDs after a fork() call. We assume this
  /// never happens.
  ///
  /// \return A random object id.
  static ObjectID FromRandom();

  /// Compute the object ID that is used to track an actor's lifetime. This
  /// object does not actually have a value; it is just used for counting
  /// references (handles) to the actor.
  ///
  /// \param actor_id The ID of the actor to track.
  /// \return The computed object ID.
  static ObjectID ForActorHandle(const ActorID &actor_id);

  MSGPACK_DEFINE(id_);

 private:
  /// A helper method to generate an ObjectID.
  static ObjectID GenerateObjectId(const std::string &task_id_binary,
                                   ObjectIDFlagsType flags,
                                   ObjectIDIndexType object_index = 0);

  /// Get the flags out of this object id.
  ObjectIDFlagsType GetFlags() const;

 private:
  uint8_t id_[kLength];
};

static_assert(sizeof(JobID) == JobID::kLength + sizeof(size_t),
              "JobID size is not as expected");
static_assert(sizeof(ActorID) == ActorID::kLength + sizeof(size_t),
              "ActorID size is not as expected");
static_assert(sizeof(TaskID) == TaskID::kLength + sizeof(size_t),
              "TaskID size is not as expected");
static_assert(sizeof(ObjectID) == ObjectID::kLength + sizeof(size_t),
              "ObjectID size is not as expected");

std::ostream &operator<<(std::ostream &os, const UniqueID &id);
std::ostream &operator<<(std::ostream &os, const JobID &id);
std::ostream &operator<<(std::ostream &os, const ActorID &id);
std::ostream &operator<<(std::ostream &os, const TaskID &id);
std::ostream &operator<<(std::ostream &os, const ObjectID &id);

#define DEFINE_UNIQUE_ID(type)                                                 \
  class RAY_EXPORT type : public UniqueID {                                    \
   public:                                                                     \
    explicit type(const UniqueID &from) {                                      \
      std::memcpy(&id_, from.Data(), kUniqueIDSize);                           \
    }                                                                          \
    type() : UniqueID() {}                                                     \
    static type FromRandom() { return type(UniqueID::FromRandom()); }          \
    static type FromBinary(const std::string &binary) { return type(binary); } \
    static type Nil() { return type(UniqueID::Nil()); }                        \
    static size_t Size() { return kUniqueIDSize; }                             \
                                                                               \
   private:                                                                    \
    explicit type(const std::string &binary) {                                 \
      RAY_CHECK(binary.size() == Size() || binary.size() == 0)                 \
          << "expected size is " << Size() << ", but got " << binary.size();   \
      std::memcpy(&id_, binary.data(), binary.size());                         \
    }                                                                          \
  };

#include "id_def.h"

#undef DEFINE_UNIQUE_ID

// Restore the compiler alignment to default (8 bytes).
#pragma pack(pop)

template <typename T>
BaseID<T>::BaseID() {
  // Using const_cast to directly change data is dangerous. The cached
  // hash may not be changed. This is used in construction time.
  std::fill_n(this->MutableData(), T::Size(), 0xff);
}

template <typename T>
T BaseID<T>::FromRandom() {
  std::string data(T::Size(), 0);
  FillRandom(&data);
  return T::FromBinary(data);
}

template <typename T>
T BaseID<T>::FromBinary(const std::string &binary) {
  RAY_CHECK(binary.size() == T::Size() || binary.size() == 0)
      << "expected size is " << T::Size() << ", but got " << binary.size();
  T t;
  std::memcpy(t.MutableData(), binary.data(), binary.size());
  return t;
}

template <typename T>
const T &BaseID<T>::Nil() {
  static const T nil_id;
  return nil_id;
}

template <typename T>
bool BaseID<T>::IsNil() const {
  static T nil_id = T::Nil();
  return *this == nil_id;
}

template <typename T>
size_t BaseID<T>::Hash() const {
  // Note(ashione): hash code lazy calculation(it's invoked every time if hash code is
  // default value 0)
  if (!hash_) {
    hash_ = MurmurHash64A(Data(), T::Size(), 0);
  }
  return hash_;
}

template <typename T>
bool BaseID<T>::operator==(const BaseID &rhs) const {
  return std::memcmp(Data(), rhs.Data(), T::Size()) == 0;
}

template <typename T>
bool BaseID<T>::operator!=(const BaseID &rhs) const {
  return !(*this == rhs);
}

template <typename T>
uint8_t *BaseID<T>::MutableData() {
  return reinterpret_cast<uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
const uint8_t *BaseID<T>::Data() const {
  return reinterpret_cast<const uint8_t *>(this) + sizeof(hash_);
}

template <typename T>
std::string BaseID<T>::Binary() const {
  return std::string(reinterpret_cast<const char *>(Data()), T::Size());
}

template <typename T>
std::string BaseID<T>::Hex() const {
  constexpr char hex[] = "0123456789abcdef";
  const uint8_t *id = Data();
  std::string result;
  for (int i = 0; i < T::Size(); i++) {
    unsigned int val = id[i];
    result.push_back(hex[val >> 4]);
    result.push_back(hex[val & 0xf]);
  }
  return result;
}

}  // namespace ray

namespace std {

#define DEFINE_UNIQUE_ID(type)                                           \
  template <>                                                            \
  struct hash<::ray::type> {                                             \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };                                                                     \
  template <>                                                            \
  struct hash<const ::ray::type> {                                       \
    size_t operator()(const ::ray::type &id) const { return id.Hash(); } \
  };

DEFINE_UNIQUE_ID(UniqueID);
DEFINE_UNIQUE_ID(JobID);
DEFINE_UNIQUE_ID(ActorID);
DEFINE_UNIQUE_ID(TaskID);
DEFINE_UNIQUE_ID(ObjectID);
#include "id_def.h"

#undef DEFINE_UNIQUE_ID
}  // namespace std
#endif  // RAY_ID_H_
