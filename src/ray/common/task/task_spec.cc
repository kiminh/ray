#include <sstream>

#include "ray/common/task/task_spec.h"
#include "ray/util/logging.h"

namespace ray {

void TaskSpecification::ComputeResources() {
  auto required_resources = MapFromProtobuf(message_->required_resources());
  auto required_placement_resources =
      MapFromProtobuf(message_->required_placement_resources());
  if (required_placement_resources.empty()) {
    required_placement_resources = required_resources;
  }
  required_resources_ = ResourceSet(required_resources);
  required_placement_resources_ = ResourceSet(required_placement_resources);
}

// Task specification getter methods.
TaskID TaskSpecification::TaskId() const {
  return TaskID::FromBinary(message_->task_id());
}

JobID TaskSpecification::JobId() const { return JobID::FromBinary(message_->job_id()); }

TaskID TaskSpecification::ParentTaskId() const {
  return TaskID::FromBinary(message_->parent_task_id());
}

size_t TaskSpecification::ParentCounter() const { return message_->parent_counter(); }

std::vector<std::string> TaskSpecification::FunctionDescriptor() const {
  return VectorFromProtobuf(message_->function_descriptor());
}

size_t TaskSpecification::NumArgs() const { return message_->args_size(); }

size_t TaskSpecification::NumReturns() const { return message_->num_returns(); }

ObjectID TaskSpecification::ReturnId(size_t return_index) const {
  return ObjectID::ForTaskReturn(TaskId(), return_index + 1, /*transport_type=*/0);
}

bool TaskSpecification::ArgByRef(size_t arg_index) const {
  return (ArgIdCount(arg_index) != 0);
}

size_t TaskSpecification::ArgIdCount(size_t arg_index) const {
  return message_->args(arg_index).object_ids_size();
}

ObjectID TaskSpecification::ArgId(size_t arg_index, size_t id_index) const {
  return ObjectID::FromBinary(message_->args(arg_index).object_ids(id_index));
}

const uint8_t *TaskSpecification::ArgVal(size_t arg_index) const {
  return reinterpret_cast<const uint8_t *>(message_->args(arg_index).data().data());
}

size_t TaskSpecification::ArgValLength(size_t arg_index) const {
  return message_->args(arg_index).data().size();
}

const ResourceSet TaskSpecification::GetRequiredResources() const {
  return required_resources_;
}

const ResourceSet TaskSpecification::GetRequiredPlacementResources() const {
  return required_placement_resources_;
}

bool TaskSpecification::IsDriverTask() const {
  // Driver tasks are empty tasks that have no function ID set.
  return FunctionDescriptor().empty();
}

Language TaskSpecification::GetLanguage() const { return message_->language(); }

bool TaskSpecification::IsNormalTask() const {
  return message_->type() == TaskType::NORMAL_TASK;
}

bool TaskSpecification::IsActorCreationTask() const {
  return message_->type() == TaskType::ACTOR_CREATION_TASK;
}

bool TaskSpecification::IsActorTask() const {
  return message_->type() == TaskType::ACTOR_TASK;
}

// === Below are getter methods specific to actor creation tasks.

ActorID TaskSpecification::ActorCreationId() const {
  RAY_CHECK(IsActorCreationTask());
  return ActorID::FromBinary(message_->actor_creation_task_spec().actor_id());
}

uint64_t TaskSpecification::MaxActorReconstructions() const {
  RAY_CHECK(IsActorCreationTask());
  return message_->actor_creation_task_spec().max_actor_reconstructions();
}

std::vector<std::string> TaskSpecification::DynamicWorkerOptions() const {
  RAY_CHECK(IsActorCreationTask());
  return VectorFromProtobuf(
      message_->actor_creation_task_spec().dynamic_worker_options());
}

// === Below are getter methods specific to actor tasks.

ActorID TaskSpecification::ActorId() const {
  RAY_CHECK(IsActorTask());
  return ActorID::FromBinary(message_->actor_task_spec().actor_id());
}

ActorHandleID TaskSpecification::ActorHandleId() const {
  RAY_CHECK(IsActorTask());
  return ActorHandleID::FromBinary(message_->actor_task_spec().actor_handle_id());
}

uint64_t TaskSpecification::ActorCounter() const {
  RAY_CHECK(IsActorTask());
  return message_->actor_task_spec().actor_counter();
}

ObjectID TaskSpecification::ActorCreationDummyObjectId() const {
  RAY_CHECK(IsActorTask());
  return ObjectID::FromBinary(
      message_->actor_task_spec().actor_creation_dummy_object_id());
}

ObjectID TaskSpecification::PreviousActorTaskDummyObjectId() const {
  RAY_CHECK(IsActorTask());
  return ObjectID::FromBinary(
      message_->actor_task_spec().previous_actor_task_dummy_object_id());
}

ObjectID TaskSpecification::ActorDummyObject() const {
  RAY_CHECK(IsActorTask() || IsActorCreationTask());
  return ReturnId(NumReturns() - 1);
}

std::vector<ActorHandleID> TaskSpecification::NewActorHandles() const {
  RAY_CHECK(IsActorTask());
  return IdVectorFromProtobuf<ActorHandleID>(
      message_->actor_task_spec().new_actor_handles());
}

std::string TaskSpecification::DebugString() const {
  std::ostringstream stream;
  stream << "Type=" << TaskType_Name(message_->type())
         << ", Language=" << Language_Name(message_->language())
         << ", function_descriptor=";

  // Print function descriptor.
  const auto list = VectorFromProtobuf(message_->function_descriptor());
  // The 4th is the code hash which is binary bits. No need to output it.
  const size_t size = std::min(static_cast<size_t>(3), list.size());
  for (size_t i = 0; i < size; ++i) {
    if (i != 0) {
      stream << ",";
    }
    stream << list[i];
  }

  stream << ", task_id=" << TaskId() << ", job_id=" << JobId()
         << ", num_args=" << NumArgs() << ", num_returns=" << NumReturns();

  if (IsActorCreationTask()) {
    // Print actor creation task spec.
    stream << ", actor_creation_task_spec={actor_id=" << ActorCreationId()
           << ", max_reconstructions=" << MaxActorReconstructions() << "}";
  } else if (IsActorTask()) {
    // Print actor task spec.
    stream << ", actor_task_spec={actor_id=" << ActorId()
           << ", actor_handle_id=" << ActorHandleId()
           << ", actor_counter=" << ActorCounter() << "}";
  }

  return stream.str();
}

rapidjson::Document TaskSpecification::ToJson(
    rapidjson::Document::AllocatorType *allocator) const {
  rapidjson::Document doc(rapidjson::kObjectType, allocator);
  rapidjson::Document::AllocatorType &alloc = doc.GetAllocator();

  doc.AddMember("language", Language_Name(message_->language()), alloc);
  doc.AddMember("task id", TaskId().Hex(), alloc);
  doc.AddMember("job id", JobId().Hex(), alloc);
  doc.AddMember("num args", static_cast<uint64_t>(NumArgs()), alloc);
  doc.AddMember("num returns", static_cast<uint64_t>(NumReturns()), alloc);

  rapidjson::Document function_descriptor_doc(rapidjson::kArrayType, &alloc);
  // Print function descriptor.
  const auto list = VectorFromProtobuf(message_->function_descriptor());
  // The 4th is the code hash which is binary bits. No need to output it.
  const size_t size = std::min(static_cast<size_t>(3), list.size());
  for (int i = 0; i < size; ++i) {
    rapidjson::Value v(list[i], alloc);
    function_descriptor_doc.PushBack(v, alloc);
  }
  doc.AddMember("function descriptor", function_descriptor_doc, alloc);

  if (IsActorCreationTask()) {
    // Print actor creation task spec.
    rapidjson::Document actor_creation_doc(rapidjson::kObjectType, &alloc);
    actor_creation_doc.AddMember("actor id", ActorCreationId().Hex(), alloc);
    actor_creation_doc.AddMember("max reconstructions", MaxActorReconstructions(), alloc);
    doc.AddMember("actor creation task spec", actor_creation_doc, alloc);
  } else if (IsActorTask()) {
    // Print actor task spec.
    rapidjson::Document actor_doc(rapidjson::kObjectType, &alloc);
    actor_doc.AddMember("actor id", ActorId().Hex(), alloc);
    actor_doc.AddMember("actor handle id", ActorHandleId().Hex(), alloc);
    actor_doc.AddMember("actor counter", ActorCounter(), alloc);
    doc.AddMember("actor task spec", actor_doc, alloc);
  }

  return doc;
}

}  // namespace ray
