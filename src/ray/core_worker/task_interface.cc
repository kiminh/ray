#include "ray/raylet/task.h"
#include "context.h"
#include "core_worker.h"
#include "task_interface.h"

namespace ray {

Status CoreWorkerTaskInterface::SubmitTask(const RayFunction &function,
                                           const std::vector<TaskArg> &args,
                                           const TaskOptions &task_options,
                                           std::vector<ObjectID> *return_ids) {
  auto &context = core_worker_.worker_context_;
  auto next_task_index = context.GetNextTaskIndex();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
                                      context.GetCurrentTaskID(), next_task_index);

  auto num_returns = task_options.num_returns;
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::ForTaskReturn(task_id, i + 1);
  }

  auto task_arguments = BuildTaskArguments(args);
  auto ret = ToTaskLanguage(function.language);
  if (!ret.first.ok()) {
    RAY_LOG(ERROR) << ret.first.message() << "  task: " << task_id
                   << ", language: " << static_cast<int>(function.language);
    return ret.first;
  }

  auto language = ret.second;
  ray::raylet::TaskSpecification spec(context.GetCurrentDriverID(),
                                      context.GetCurrentTaskID(), next_task_index,
                                      task_arguments, num_returns, task_options.resources,
                                      language, function.function_descriptor);

  std::vector<ObjectID> execution_dependencies;
  return core_worker_.raylet_client_->SubmitTask(execution_dependencies, spec);
}

Status CoreWorkerTaskInterface::CreateActor(
    const RayFunction &function, const std::vector<TaskArg> &args,
    const ActorCreationOptions &actor_creation_options,
    std::unique_ptr<ActorHandle> *actor_handle) {
  auto &context = core_worker_.worker_context_;
  auto next_task_index = context.GetNextTaskIndex();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
                                      context.GetCurrentTaskID(), next_task_index);

  std::vector<ObjectID> return_ids;
  return_ids.push_back(ObjectID::ForTaskReturn(task_id, 1));
  ActorID actor_creation_id = ActorID::FromBinary(return_ids[0].Binary());

  *actor_handle = std::unique_ptr<ActorHandle>(
      new ActorHandle(actor_creation_id, ActorHandleID::Nil()));
  (*actor_handle)->IncreaseTaskCounter();
  (*actor_handle)->SetActorCursor(return_ids[0]);

  auto task_arguments = BuildTaskArguments(args);
  auto ret = ToTaskLanguage(function.language);
  if (!ret.first.ok()) {
    RAY_LOG(ERROR) << ret.first.message() << "  task: " << task_id
                   << ", language: " << static_cast<int>(function.language);
    return ret.first;
  }

  auto language = ret.second;

  // Note that the caller is supposed to specify required placement resources
  // correctly via actor_creation_options.resources.
  ray::raylet::TaskSpecification spec(
      context.GetCurrentDriverID(), context.GetCurrentTaskID(), next_task_index,
      actor_creation_id, ObjectID::Nil(), actor_creation_options.max_reconstructions,
      ActorID::Nil(), ActorHandleID::Nil(), 0, {}, task_arguments, 1,
      actor_creation_options.resources, actor_creation_options.resources, language,
      function.function_descriptor);

  std::vector<ObjectID> execution_dependencies;
  return core_worker_.raylet_client_->SubmitTask(execution_dependencies, spec);
}

Status CoreWorkerTaskInterface::SubmitActorTask(ActorHandle &actor_handle,
                                                const RayFunction &function,
                                                const std::vector<TaskArg> &args,
                                                const TaskOptions &task_options,
                                                std::vector<ObjectID> *return_ids) {
  auto &context = core_worker_.worker_context_;
  auto next_task_index = context.GetNextTaskIndex();
  const auto task_id = GenerateTaskId(context.GetCurrentDriverID(),
                                      context.GetCurrentTaskID(), next_task_index);

  // add one for actor cursor object id.
  auto num_returns = task_options.num_returns + 1;
  (*return_ids).resize(num_returns);
  for (int i = 0; i < num_returns; i++) {
    (*return_ids)[i] = ObjectID::ForTaskReturn(task_id, i + 1);
  }

  auto actor_creation_dummy_object_id =
      ObjectID::FromBinary(actor_handle.ActorID().Binary());

  auto task_arguments = BuildTaskArguments(args);
  auto ret = ToTaskLanguage(function.language);
  if (!ret.first.ok()) {
    RAY_LOG(ERROR) << ret.first.message() << "  task: " << task_id
                   << ", language: " << static_cast<int>(function.language);
    return ret.first;
  }

  auto language = ret.second;

  std::vector<ActorHandleID> new_actor_handles;
  ray::raylet::TaskSpecification spec(
      context.GetCurrentDriverID(), context.GetCurrentTaskID(), next_task_index,
      ActorID::Nil(), actor_creation_dummy_object_id, 0, actor_handle.ActorID(),
      actor_handle.ActorHandleID(), actor_handle.IncreaseTaskCounter(), new_actor_handles,
      task_arguments, num_returns, task_options.resources, task_options.resources, language,
      function.function_descriptor);

  std::vector<ObjectID> execution_dependencies;
  execution_dependencies.push_back(actor_handle.ActorCursor());

  auto actor_cursor = (*return_ids).back();
  actor_handle.SetActorCursor(actor_cursor);
  actor_handle.ClearNewActorHandles();

  auto status = core_worker_.raylet_client_->SubmitTask(execution_dependencies, spec);

  // remove cursor from return ids.
  (*return_ids).pop_back();
  return status;
}

std::vector<std::shared_ptr<raylet::TaskArgument>>
CoreWorkerTaskInterface::BuildTaskArguments(const std::vector<TaskArg> &args) {
  std::vector<std::shared_ptr<raylet::TaskArgument>> task_arguments;
  for (auto &arg : args) {
    if (arg.IsPassedByReference()) {
      std::vector<ObjectID> references{arg.GetReference()};
      task_arguments.push_back(
          std::make_shared<raylet::TaskArgumentByReference>(references));
    } else {
      auto data = arg.GetValue();
      task_arguments.push_back(
          std::make_shared<raylet::TaskArgumentByValue>(data->Data(), data->Size()));
    }
  }
  return task_arguments;
}

std::pair<Status, ::Language> CoreWorkerTaskInterface::ToTaskLanguage(WorkerLanguage language) {
  switch(language) {
  case ray::WorkerLanguage::JAVA:
    return std::make_pair(Status::OK(), ::Language::JAVA);
    break;
  case ray::WorkerLanguage::PYTHON:
    return std::make_pair(Status::OK(), ::Language::PYTHON);
    break;
  default:
    return std::make_pair(Status::Invalid("invalid language specified"), ::Language::PYTHON);
    break;
  }
}

}  // namespace ray
