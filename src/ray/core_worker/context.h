#ifndef RAY_CORE_WORKER_CONTEXT_H
#define RAY_CORE_WORKER_CONTEXT_H

#include "ray/common/task/task_spec.h"
#include "ray/core_worker/common.h"

namespace ray {

struct WorkerThreadContext;

class WorkerContext {
 public:
  WorkerContext(WorkerType worker_type, const JobID &job_id, const std::string &raylet_socket);

  const WorkerType GetWorkerType() const;

  const WorkerID &GetWorkerID() const;

  const JobID &GetCurrentJobID() const;

  const TaskID &GetCurrentTaskID() const;

  void SetCurrentTask(const TaskSpecification &task_spec);

  std::shared_ptr<const TaskSpecification> GetCurrentTask() const;

  const ActorID &GetCurrentActorID() const;

  int GetNextTaskIndex();

  int GetNextPutIndex();

  std::shared_ptr<RayletClient> GetRayletClient();

 private:
  /// Type of the worker process. Whether it's a driver or worker.
  const WorkerType worker_type_;
  /// The job ID which is only valid when it's a driver.
  const JobID job_id_;
  /// raylet socket name.
  const std::string raylet_socket_;

 private:
  static WorkerThreadContext &GetThreadContext();

  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;
};

class RuntimeContext {
 public:
  RuntimeContext(const std::string &raylet_socket);
 
  static std::shared_ptr<RayletClient> GetThreadRayletClient();
 private:
  /// Per-thread worker context.
  static thread_local std::unique_ptr<WorkerThreadContext> thread_context_;  
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CONTEXT_H
