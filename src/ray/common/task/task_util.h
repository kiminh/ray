#ifndef RAY_COMMON_TASK_TASK_UTIL_H
#define RAY_COMMON_TASK_TASK_UTIL_H

#include "ray/common/buffer.h"
#include "ray/common/ray_object.h"
#include "ray/common/common_protocol.h"
#include "ray/common/task/task_spec.h"
#include "ray/protobuf/common.pb.h"

namespace ray {

/// Helper class for building a `TaskSpecification` object.
class TaskSpecBuilder {
 public:
  TaskSpecBuilder() {}

  /// Build the `TaskSpecification` object.
  TaskSpecification Build() {
    // Note that this should be invoked before task_spec_builder created.
    auto task_args_offset = fbb_.CreateVector(task_args_);

//    task_spec_builder_ = std::make_shared<rpc::flatbuf::TaskSpecBuilder>(fbb_);
//    task_spec_builder_->add_type(type_);
//    task_spec_builder_->add_language(language_);
//    task_spec_builder_->add_function_descriptor(function_descriptor_);
//    task_spec_builder_->add_job_id(to_flatbuf<JobID>(fbb_, job_id_));
//    task_spec_builder_->add_task_id(to_flatbuf(task_id_));
//    task_spec_builder_->add_parent_task_id(to_flatbuf(parent_task_id_));
//    task_spec_builder_->add_parent_counter(parent_counter_);
//    task_spec_builder_->add_caller_id(to_flatbuf(caller_id_));
//    task_spec_builder_->add_caller_address(caller_address_);
//    task_spec_builder_->add_num_returns(num_returns_);
//    task_spec_builder_->add_required_resources(required_resources_);
//    task_spec_builder_->add_required_placement_resources(required_placement_resources_);
//    task_spec_builder_->add_actor_creation_task_spec(actor_creation_task_spec_);
//    task_spec_builder_->add_actor_task_spec(actor_task_spec_);
//    task_spec_builder_->add_max_retries(max_retries_);
//    task_spec_builder_->add_args(task_args_offset);
    auto spec = rpc::flatbuf::CreateTaskSpec(
        fbb_,
        /*type=*/type_,
        /*language=*/language_,
        /*function_descriptor=*/function_descriptor_,
        /*job_id=*/to_flatbuf(fbb_, job_id_),
        /*task_id=*/to_flatbuf(fbb_, task_id_),
        /*parent_task_id=*/to_flatbuf(fbb_, parent_task_id_),
        parent_counter_,
        /*caller_id*/to_flatbuf(fbb_, caller_id_),
        /*caller_address=*/caller_address_,
        /*task_agrs=*/task_args_offset,
        /*num_returns=*/num_returns_,
        /*required_resources=*/required_resources_,
        /*required_placement_resources=*/required_placement_resources_,
        /*actor_creation_task_spec_=*/actor_creation_task_spec_,
        /*actor_task_spec_=*/actor_task_spec_,
        /*max_retries=*/max_retries_);
    fbb_.Finish(spec);
    return TaskSpecification(fbb_.GetBufferPointer(), fbb_.GetSize());
  }

  /// Set the common attributes of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetCommonTaskSpec(
      const TaskID &task_id, const Language &language,
      const ray::FunctionDescriptor &function_descriptor, const JobID &job_id,
      const TaskID &parent_task_id, uint64_t parent_counter, const TaskID &caller_id,
      const rpc::Address &caller_address, uint64_t num_returns,
      const std::unordered_map<std::string, double> &required_resources,
      const std::unordered_map<std::string, double> &required_placement_resources) {

    type_ = rpc::flatbuf::TaskType::NORMAL_TASK;
    language_ = ToFlatbufLanguage(language);
    function_descriptor_ = string_to_flatbuf(fbb_, function_descriptor->Serialize());
    job_id_ = job_id;
    task_id_ = task_id;
    parent_task_id_ = parent_task_id;
    parent_counter_ = parent_counter;
    caller_id_ = caller_id;
    std::string address_str;
    caller_address.SerializeToString(&address_str);
    caller_address_ = string_to_flatbuf(fbb_, address_str);
    num_returns_ = num_returns;
    required_resources_ = ToFlatbufResources(fbb_, required_resources);
    required_placement_resources_ = ToFlatbufResources(fbb_, required_placement_resources);
    return *this;
  }

  /// Set the driver attributes of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetDriverTaskSpec(const TaskID &task_id, const Language &language,
                                     const JobID &job_id, const TaskID &parent_task_id,
                                     const TaskID &caller_id,
                                     const rpc::Address &caller_address) {
    type_ = rpc::flatbuf::TaskType::DRIVER_TASK;
    language_ = ToFlatbufLanguage(language);
    job_id_ = job_id;
    task_id_ = parent_task_id;
    parent_task_id_ = parent_task_id;
    parent_counter_ = 0;
    caller_id_ = caller_id;
    std::string address_str;
    caller_address.SerializeToString(&address_str);
    caller_address_ = string_to_flatbuf(fbb_, address_str);
    num_returns_ = 0;
    return *this;
  }

  /// Add a by-reference argument to the task.
  ///
  /// \param arg_id Id of the argument.
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &AddByRefArg(const ObjectID &arg_id) {
    std::unordered_set<ObjectID> arg_ids = {arg_id};
    auto task_arg = rpc::flatbuf::CreateTaskArg(
        fbb_,
        /*object_ids=*/to_flatbuf(fbb_, arg_ids),
        /*data=*/0,
        /*metadata=*/0,
        /*nested_inlined_ids=*/0);
    task_args_.push_back(task_arg);
    return *this;
  }

  /// Add a by-value argument to the task.
  ///
  /// \param value the RayObject instance that contains the data and the metadata.
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &AddByValueArg(const RayObject &value) {
    std::unordered_set<ObjectID> nested_ids;
    for (const auto &nested_id : value.GetNestedIds()) {
      nested_ids.insert(nested_id);
    }

    flatbuffers::Offset<flatbuffers::String> data_offset = 0;
    flatbuffers::Offset<flatbuffers::String> metadata_offset = 0;
    if (value.HasData()) {
      const auto &data = value.GetData();
      // Avoid this minor copy.
      data_offset = fbb_.CreateString(std::string(reinterpret_cast<const char *>(data->Data()), data->Size()));
    }
    if (value.HasMetadata()) {
      const auto &metadata = value.GetMetadata();
      metadata_offset = fbb_.CreateString(std::string(reinterpret_cast<const char *>(metadata->Data()), metadata->Size()));
    }

    auto task_arg = rpc::flatbuf::CreateTaskArg(
        fbb_,
        /*object_ids=*/0,
        /*data=*/data_offset,
        /*metadata=*/metadata_offset,
        /*nested_inlined_ids=*/to_flatbuf(fbb_, nested_ids));
    task_args_.push_back(task_arg);
    return *this;
  }

  /// Set the `ActorCreationTaskSpec` of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetActorCreationTaskSpec(
      const ActorID &actor_id, uint64_t max_reconstructions = 0,
      const std::vector<std::string> &dynamic_worker_options = {},
      int max_concurrency = 1, bool is_detached = false, bool is_asyncio = false) {


    type_ = rpc::flatbuf::TaskType::ACTOR_CREATION_TASK;
    actor_creation_task_spec_ = rpc::flatbuf::CreateActorCreationTaskSpec(
        fbb_,
        /*actor_id=*/to_flatbuf(fbb_, actor_id),
        /*max_actor_reconstructions=*/max_concurrency,
        /*dynamic_worker_options=*/string_vec_to_flatbuf(fbb_, dynamic_worker_options),
        /*is_asyncio=*/is_asyncio,
        /*is_detached=*/is_detached
    );
    return *this;
  }

  /// Set the `ActorTaskSpec` of the task spec.
  /// See `common.proto` for meaning of the arguments.
  ///
  /// \return Reference to the builder object itself.
  TaskSpecBuilder &SetActorTaskSpec(const ActorID &actor_id,
                                    const ObjectID &actor_creation_dummy_object_id,
                                    const ObjectID &previous_actor_task_dummy_object_id,
                                    uint64_t actor_counter) {
    type_ = rpc::flatbuf::TaskType::ACTOR_TASK;
    actor_task_spec_ = rpc::flatbuf::CreateActorTaskSpec(
        fbb_,
        /*actor_id=*/to_flatbuf(fbb_, actor_id),
        /*actor_creation_dummy_object_id=*/to_flatbuf(fbb_, actor_creation_dummy_object_id),
        /*actor_counter=*/static_cast<int32_t>(actor_counter),
        /*previous_actor_task_dummy_object_id=*/to_flatbuf(fbb_, previous_actor_task_dummy_object_id)
        );
    return *this;
  }

 private:
      rpc::flatbuf::TaskType type_ = rpc::flatbuf::TaskType::NORMAL_TASK;
      rpc::flatbuf::Language language_ = rpc::flatbuf::Language::JAVA;
      flatbuffers::Offset<flatbuffers::String> function_descriptor_ = 0;
      JobID job_id_;
      TaskID task_id_;
      TaskID parent_task_id_;
      int32_t parent_counter_;
      TaskID caller_id_;
      flatbuffers::Offset<flatbuffers::String> caller_address_ = 0;
      std::vector<flatbuffers::Offset<rpc::flatbuf::TaskArg>> task_args_;
      int32_t num_returns_ = 0;
      flatbuffers::Offset<rpc::flatbuf::Resources> required_resources_ = 0;
      flatbuffers::Offset<rpc::flatbuf::Resources> required_placement_resources_ = 0;
      flatbuffers::Offset<rpc::flatbuf::ActorCreationTaskSpec> actor_creation_task_spec_ = 0;
      flatbuffers::Offset<rpc::flatbuf::ActorTaskSpec> actor_task_spec_ = 0;
      int32_t max_retries_ = 0;


  flatbuffers::FlatBufferBuilder fbb_;
  std::shared_ptr<rpc::flatbuf::TaskSpecBuilder> task_spec_builder_;
};

}  // namespace ray

#endif  // RAY_COMMON_TASK_TASK_UTIL_H
