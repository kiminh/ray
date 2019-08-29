#ifndef RAY_CORE_WORKER_TASK_EXECUTION_H
#define RAY_CORE_WORKER_TASK_EXECUTION_H

#include "ray/common/buffer.h"
#include "ray/common/status.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/transport/transport.h"
#include "ray/rpc/client_call.h"
#include "ray/rpc/worker/worker_client.h"
#include "ray/rpc/worker/worker_server.h"

namespace ray {

class CoreWorker;

namespace raylet {
class TaskSpecification;
}

/// The interface that contains all `CoreWorker` methods that are related to task
/// execution.
class CoreWorkerTaskExecutionInterface {
 public:
  /// The callback provided app-language workers that executes tasks.
  ///
  /// \param ray_function[in] Information about the function to execute.
  /// \param args[in] Arguments of the task.
  /// \param results[out] Results of the task execution.
  /// \return Status.
  using TaskExecutor = std::function<Status(
      const RayFunction &ray_function,
      const std::vector<std::shared_ptr<RayObject>> &args, int num_returns,
      std::vector<std::shared_ptr<RayObject>> *results)>;

  CoreWorkerTaskExecutionInterface(
      CoreWorkerStoreProviderMap &store_providers,
      std::shared_ptr<boost::asio::io_service> io_service,
      std::unordered_map<WorkerID, std::shared_ptr<boost::asio::io_service>> &worker_main_services,
      const TaskExecutor &executor,
      bool use_asio_rpc);

  /// Start receiving and executing tasks.
  /// \return void.
  void Run();

  /// Stop receiving and executing tasks.
  /// \return void.
  void Stop();

 private:
  /// Build arguments for task executor. This would loop through all the arguments
  /// in task spec, and for each of them that's passed by reference (ObjectID),
  /// fetch its content from store and; for arguments that are passed by value,
  /// just copy their content.
  ///
  /// \param spec[in] Task specification.
  /// \param args[out] The arguments for passing to task executor.
  ///
  Status BuildArgsForExecutor(const TaskSpecification &spec,
                              std::vector<std::shared_ptr<RayObject>> *args);

  /// Execute a task.
  ///
  /// \param spec[in] Task specification.
  /// \param results[out] Results for task execution.
  /// \return Status.
  Status ExecuteTask(const TaskSpecification &spec,
                     std::vector<std::shared_ptr<RayObject>> *results);


  CoreWorkerStoreProviderMap &store_providers_;

  std::shared_ptr<boost::asio::io_service> io_service_;

  std::unordered_map<WorkerID, std::shared_ptr<boost::asio::io_service>> &worker_main_services_;

  // Task execution callback.
  TaskExecutor execution_callback_;

  /// All the task task receivers supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskReceiver>>
      task_receivers_;

  /// The RPC server.
  std::unique_ptr<rpc::RpcServer> worker_server_;

  friend class CoreWorkerProcess;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_TASK_EXECUTION_H
