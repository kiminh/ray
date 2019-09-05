#ifndef RAY_CORE_WORKER_CORE_WORKER_H
#define RAY_CORE_WORKER_CORE_WORKER_H

#include "ray/common/buffer.h"
#include "ray/core_worker/common.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/object_interface.h"
#include "ray/core_worker/task_execution.h"
#include "ray/core_worker/task_interface.h"
#include "ray/gcs/redis_gcs_client.h"
#include "ray/raylet/raylet_client.h"

#include "ray/core_worker/store_provider/store_provider.h"
#include "ray/core_worker/transport/transport.h"

namespace ray {

class CoreWorker;

/// The root class that contains all the core and language-independent functionalities
/// of the worker process. This class is supposed to be used to implement app-language
/// (Java, Python, etc) workers.
class CoreWorkerProcess {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] worker_type Type of this worker.
  /// \param[in] langauge Language of this worker.
  CoreWorkerProcess(
      const WorkerType worker_type, const Language language,
      const std::unordered_map<std::string, std::string> &static_worker_info,
      const std::string &store_socket, const std::string &raylet_socket,
      const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
      const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback,
      int num_workers = 1);

  ~CoreWorkerProcess();

  /// Type of this worker.
  WorkerType GetWorkerType() const { return worker_type_; }

  /// Language of this worker.
  Language GetLanguage() const { return language_; }

  const JobID &GetJobId() const { return job_id_; }

  const std::string &GetRayletSocket() const { return raylet_socket_; }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return *task_interface_; }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return *object_interface_; }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() {
    RAY_CHECK(task_execution_interface_ != nullptr);
    return *task_execution_interface_;
  }

  static std::shared_ptr<CoreWorker> GetCoreWorker() {
    RAY_CHECK(current_core_worker_ != nullptr);
    return current_core_worker_;
  }

  static void SetCoreWorker(std::shared_ptr<CoreWorker> core_worker) {
    current_core_worker_ = core_worker;
  }

  void SetCoreWorker(const WorkerID &worker_id) {
    auto it = core_workers_.find(worker_id);
    RAY_CHECK(it != core_workers_.end()) << "Worker " << worker_id << " not found.";
    SetCoreWorker(it->second);
  }

 private:
  void StartIOService();

  void InitializeStoreProviders();
  void InitializeTaskSubmitters(bool use_asio_rpc);

  /// Register this worker or driver to GCS.
  void RegisterWorker(const WorkerID &worker_id);

  std::unique_ptr<CoreWorkerStoreProvider> CreateStoreProvider(StoreProviderType type);

  /// Type of this worker.
  const WorkerType worker_type_;

  /// Language of this worker.
  const Language language_;

  /// Static worker info used to register worker to GCS.
  const std::unordered_map<std::string, std::string> static_worker_info_;

  /// plasma store socket name.
  const std::string store_socket_;

  /// raylet socket name.
  const std::string raylet_socket_;

  /// Initial job ID.
  const JobID job_id_;

  /// event loop where the IO events are handled. e.g. async GCS operations.
  std::shared_ptr<boost::asio::io_service> io_service_;

  /// keeps io_service_ alive.
  boost::asio::io_service::work io_work_;

  /// The thread to handle IO events.
  std::thread io_thread_;

  /// GCS client.
  std::unique_ptr<gcs::RedisGcsClient> gcs_client_;

  /// In-memory store for return objects. This is used for `MEMORY` store provider.
  std::shared_ptr<CoreWorkerMemoryStore> memory_store_;

  /// All the store providers supported.
  EnumUnorderedMap<StoreProviderType, std::unique_ptr<CoreWorkerStoreProvider>>
      store_providers_;

  /// All the task submitters supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskSubmitter>>
      task_submitters_;

  /// All the task task receivers supported.
  EnumUnorderedMap<TaskTransportType, std::unique_ptr<CoreWorkerTaskReceiver>>
      task_receivers_;

  /// The `CoreWorkerTaskInterface` instance.
  std::unique_ptr<CoreWorkerTaskInterface> task_interface_;

  /// The `CoreWorkerObjectInterface` instance.
  std::unique_ptr<CoreWorkerObjectInterface> object_interface_;

  /// The `CoreWorkerTaskExecutionInterface` instance.
  /// This is only available if it's not a driver.
  std::unique_ptr<CoreWorkerTaskExecutionInterface> task_execution_interface_;

  /// Map from worker ID to worker.
  std::unordered_map<WorkerID, std::shared_ptr<CoreWorker>> core_workers_;

  static thread_local std::shared_ptr<CoreWorker> current_core_worker_;
};

/// A worker process as represented by `CoreWorkerProcess` can contain multiple workers,
/// this class represent one such worker.
class CoreWorker {
 public:
  /// Construct a CoreWorker instance.
  ///
  /// \param[in] core_worker_process The core worker process.
  /// \param[in] worker_id ID of this worker.
  CoreWorker(CoreWorkerProcess &core_worker_process, const WorkerID &worker_id);

  void ConnectToRaylet(int rpc_server_port);

  ~CoreWorker();

  /// Type of this worker.
  WorkerType GetWorkerType() const { return core_worker_process_.GetWorkerType(); }

  /// Language of this worker.
  Language GetLanguage() const { return core_worker_process_.GetLanguage(); }

  WorkerContext &GetWorkerContext() { return worker_context_; }

  RayletClient &GetRayletClient() {
    RAY_CHECK(raylet_client_);
    return *raylet_client_;
  }

  const WorkerID &GetWorkerID() const { return worker_context_.GetWorkerID(); }

  /// Return the `CoreWorkerTaskInterface` that contains the methods related to task
  /// submisson.
  CoreWorkerTaskInterface &Tasks() { return core_worker_process_.Tasks(); }

  /// Return the `CoreWorkerObjectInterface` that contains methods related to object
  /// store.
  CoreWorkerObjectInterface &Objects() { return core_worker_process_.Objects(); }

  /// Return the `CoreWorkerTaskExecutionInterface` that contains methods related to
  /// task execution.
  CoreWorkerTaskExecutionInterface &Execution() {
    return core_worker_process_.Execution();
  }

  std::shared_ptr<boost::asio::io_service> GetMainService() { return main_service_; }

 private:
  CoreWorkerProcess &core_worker_process_;

  /// Worker context.
  WorkerContext worker_context_;

  /// Raylet client.
  std::unique_ptr<RayletClient> raylet_client_;

  /// Event loop where tasks are processed.
  std::shared_ptr<boost::asio::io_service> main_service_;

  /// The asio work to keep main_service_ alive.
  boost::asio::io_service::work main_work_;
};

}  // namespace ray

#endif  // RAY_CORE_WORKER_CORE_WORKER_H
