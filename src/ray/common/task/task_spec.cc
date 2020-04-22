#include <sstream>

#include "ray/common/task/task_spec.h"
#include "ray/util/logging.h"
#include "ray/common/common_protocol.h"

namespace ray {

absl::Mutex TaskSpecification::mutex_;
std::unordered_map<SchedulingClassDescriptor, SchedulingClass>
    TaskSpecification::sched_cls_to_id_;
std::unordered_map<SchedulingClass, SchedulingClassDescriptor>
    TaskSpecification::sched_id_to_cls_;
int TaskSpecification::next_sched_id_;

SchedulingClassDescriptor &TaskSpecification::GetSchedulingClassDescriptor(
    SchedulingClass id) {
  absl::MutexLock lock(&mutex_);
  auto it = sched_id_to_cls_.find(id);
  RAY_CHECK(it != sched_id_to_cls_.end()) << "invalid id: " << id;
  return it->second;
}

/// A helper to covert resources to map.
inline std::unordered_map<std::string, double> MapFromFlatbufResources(const rpc::flatbuf::Resources *resources) {
  auto *resource_names = resources->resource_names();
  auto *capacities = resources->resource_capacities();
  std::unordered_map<std::string, double> result;
  for (size_t i = 0; i < resource_names->size(); ++i) {
    auto name = string_from_flatbuf(*(*resource_names)[i]);
    result[name] = (*capacities)[i];
  }
  return result;
}

void TaskSpecification::ComputeResources() {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  auto required_resources = MapFromFlatbufResources(message->required_resources());
  auto required_placement_resources =
      MapFromFlatbufResources(message->required_placement_resources());
  if (required_placement_resources.empty()) {
    required_placement_resources = required_resources;
  }

  if (required_resources.empty()) {
    required_resources_ = ResourceSet::Nil();
  } else {
    required_resources_.reset(new ResourceSet(required_resources));
  }

  if (required_placement_resources.empty()) {
    required_placement_resources_ = ResourceSet::Nil();
  } else {
    required_placement_resources_.reset(new ResourceSet(required_placement_resources));
  }

  if (!IsActorTask()) {
    // Map the scheduling class descriptor to an integer for performance.
    auto sched_cls = std::make_pair(GetRequiredResources(), FunctionDescriptor());
    absl::MutexLock lock(&mutex_);
    auto it = sched_cls_to_id_.find(sched_cls);
    if (it == sched_cls_to_id_.end()) {
      sched_cls_id_ = ++next_sched_id_;
      // TODO(ekl) we might want to try cleaning up task types in these cases
      if (sched_cls_id_ > 100) {
        RAY_LOG(WARNING) << "More than " << sched_cls_id_
                         << " types of tasks seen, this may reduce performance.";
      } else if (sched_cls_id_ > 1000) {
        RAY_LOG(ERROR) << "More than " << sched_cls_id_
                       << " types of tasks seen, this may reduce performance.";
      }
      sched_cls_to_id_[sched_cls] = sched_cls_id_;
      sched_id_to_cls_[sched_cls_id_] = sched_cls;
    } else {
      sched_cls_id_ = it->second;
    }
  }
}

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<TaskID>(*message->task_id());

}

JobID TaskSpecification::JobId() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<JobID>(*message->job_id());
}

TaskID TaskSpecification::ParentTaskId() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<TaskID>(*message->parent_task_id());
}

size_t TaskSpecification::ParentCounter() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return static_cast<size_t>(message->parent_counter());

}

ray::FunctionDescriptor TaskSpecification::FunctionDescriptor() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  // Avoid this copy.
  return ray::FunctionDescriptorBuilder::Deserialize(
      std::string(message->function_descriptor()->data(), message->function_descriptor()->size()));
}

const SchedulingClass TaskSpecification::GetSchedulingClass() const {
  RAY_CHECK(sched_cls_id_ > 0);
  return sched_cls_id_;
}

size_t TaskSpecification::NumArgs() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return static_cast<size_t>(message->args()->size());
}

size_t TaskSpecification::NumReturns() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return static_cast<size_t>(message->num_returns());
}

ObjectID TaskSpecification::ReturnId(size_t return_index,
                                     TaskTransportType transport_type) const {
  return ObjectID::ForTaskReturn(TaskId(), return_index + 1,
                                 static_cast<uint8_t>(transport_type));
}

bool TaskSpecification::ArgByRef(size_t arg_index) const {
  return (ArgIdCount(arg_index) != 0);
}

size_t TaskSpecification::ArgIdCount(size_t arg_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return (*message->args())[arg_index]->object_ids()->size();
}


ObjectID TaskSpecification::ArgId(size_t arg_index, size_t id_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  const auto &object_ids = from_flatbuf<ObjectID>(*message->args()->Get(arg_index)->object_ids());
  return object_ids[id_index];
}

const uint8_t *TaskSpecification::ArgData(size_t arg_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return reinterpret_cast<const uint8_t *>(message->args()->Get(arg_index)->data()->c_str());
}

size_t TaskSpecification::ArgDataSize(size_t arg_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->args()->Get(arg_index)->data()->size();
}

const uint8_t *TaskSpecification::ArgMetadata(size_t arg_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return reinterpret_cast<const uint8_t *>(message->args()->Get(arg_index)->metadata()->c_str());
}

size_t TaskSpecification::ArgMetadataSize(size_t arg_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->args()->Get(arg_index)->metadata()->size();
}

const std::vector<ObjectID> TaskSpecification::ArgInlinedIds(size_t arg_index) const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<ObjectID>(*(message->args()->Get(arg_index)->nested_inlined_ids()));
}

const ResourceSet &TaskSpecification::GetRequiredResources() const {
  return *required_resources_;
}

std::vector<ObjectID> TaskSpecification::GetDependencies() const {
  std::vector<ObjectID> dependencies;
  for (size_t i = 0; i < NumArgs(); ++i) {
    int count = ArgIdCount(i);
    for (int j = 0; j < count; j++) {
      dependencies.push_back(ArgId(i, j));
    }
  }
  if (IsActorTask()) {
    dependencies.push_back(PreviousActorTaskDummyObjectId());
  }
  return dependencies;
}

const ResourceSet &TaskSpecification::GetRequiredPlacementResources() const {
  return *required_placement_resources_;
}

bool TaskSpecification::IsDriverTask() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->type() == rpc::flatbuf::TaskType::DRIVER_TASK;
}

Language TaskSpecification::GetLanguage() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return FromFlatbufLanguage(message->language());
}

bool TaskSpecification::IsNormalTask() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->type() == rpc::flatbuf::TaskType::NORMAL_TASK;
}

bool TaskSpecification::IsActorCreationTask() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->type() == rpc::flatbuf::TaskType::ACTOR_CREATION_TASK;
}

bool TaskSpecification::IsActorTask() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->type() == rpc::flatbuf::TaskType::ACTOR_TASK;
}

// === Below are getter methods specific to actor creation tasks.

ActorID TaskSpecification::ActorCreationId() const {
  RAY_CHECK(IsActorCreationTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return ActorID::FromBinary(string_from_flatbuf(*(message->actor_creation_task_spec()->actor_id())));
}

uint64_t TaskSpecification::MaxActorReconstructions() const {
  RAY_CHECK(IsActorCreationTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->actor_creation_task_spec()->max_actor_reconstructions();
}

std::vector<std::string> TaskSpecification::DynamicWorkerOptions() const {
  RAY_CHECK(IsActorCreationTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return string_vec_from_flatbuf(*(message->actor_creation_task_spec()->dynamic_worker_options()));
}

TaskID TaskSpecification::CallerId() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return TaskID::FromBinary(string_from_flatbuf(*(message->caller_id())));
}

const rpc::Address &TaskSpecification::CallerAddress() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  auto *caller_address = message->caller_address();
  caller_address_.ParseFromArray(caller_address->data(), caller_address->size());
  return caller_address_;
}

// === Below are getter methods specific to actor tasks.

ActorID TaskSpecification::ActorId() const {
  RAY_CHECK(IsActorTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<ActorID>(*(message->actor_task_spec()->actor_id()));
}

uint64_t TaskSpecification::ActorCounter() const {
  RAY_CHECK(IsActorTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->actor_task_spec()->actor_counter();
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  RAY_CHECK(IsActorTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<ObjectID>(*(message->actor_task_spec()->actor_creation_dummy_object_id()));
}

ObjectID TaskSpecification::PreviousActorTaskDummyObjectId() const {
  RAY_CHECK(IsActorTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return from_flatbuf<ObjectID>(*(message->actor_task_spec()->previous_actor_task_dummy_object_id()));
}

ObjectID TaskSpecification::ActorDummyObject() const {
  RAY_CHECK(IsActorTask() || IsActorCreationTask());
  return ReturnId(NumReturns() - 1, TaskTransportType::RAYLET);
}

int TaskSpecification::MaxActorConcurrency() const {
  RAY_CHECK(IsActorCreationTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->actor_creation_task_spec()->max_concurrency();
}

bool TaskSpecification::IsAsyncioActor() const {
  RAY_CHECK(IsActorCreationTask());
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return message->actor_creation_task_spec()->is_asyncio();
}

bool TaskSpecification::IsDetachedActor() const {
  auto message = flatbuffers::GetRoot<rpc::flatbuf::TaskSpec>(spec_.data());
  return IsActorCreationTask() && message->actor_creation_task_spec()->is_detached();
}

std::string TaskSpecification::DebugString() const {
  std::ostringstream stream;
//  stream << "Type=" << TaskType_Name(message_->type())
//         << ", Language=" << Language_Name(message_->language())
//         << ", function_descriptor=";

  // Print function descriptor.
  stream << FunctionDescriptor()->ToString();

  stream << ", task_id=" << TaskId() << ", job_id=" << JobId()
         << ", num_args=" << NumArgs() << ", num_returns=" << NumReturns();

  if (IsActorCreationTask()) {
    // Print actor creation task spec.
    stream << ", actor_creation_task_spec={actor_id=" << ActorCreationId()
           << ", max_reconstructions=" << MaxActorReconstructions()
           << ", max_concurrency=" << MaxActorConcurrency()
           << ", is_asyncio_actor=" << IsAsyncioActor()
           << ", is_detached=" << IsDetachedActor() << "}";
  } else if (IsActorTask()) {
    // Print actor task spec.
    stream << ", actor_task_spec={actor_id=" << ActorId()
           << ", actor_caller_id=" << CallerId() << ", actor_counter=" << ActorCounter()
           << "}";
  }

  return stream.str();
}

flatbuffers::Offset<flatbuffers::String> TaskSpecification::ToFlatbuffer(
    flatbuffers::FlatBufferBuilder &fbb) const {
  return fbb.CreateString(reinterpret_cast<const char *>(Data()), Size());
}


std::string TaskSpecification::CallSiteString() const {
  std::ostringstream stream;
  auto desc = FunctionDescriptor();
  if (IsActorCreationTask()) {
    stream << "(deserialize actor creation task arg) ";
  } else if (IsActorTask()) {
    stream << "(deserialize actor task arg) ";
  } else {
    stream << "(deserialize task arg) ";
  }
  stream << FunctionDescriptor()->CallSiteString();
  return stream.str();
}

}  // namespace ray
