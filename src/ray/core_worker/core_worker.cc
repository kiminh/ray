#include "ray/core_worker/core_worker.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/store_provider/local_plasma_provider.h"
#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/core_worker/store_provider/plasma_store_provider.h"
#include "ray/core_worker/transport/direct_actor_transport.h"
#include "ray/core_worker/transport/raylet_transport.h"

namespace ray {

thread_local std::shared_ptr<CoreWorker> CoreWorkerProcess::current_core_worker_ =
    nullptr;

CoreWorkerProcess::CoreWorkerProcess(
    const WorkerType worker_type, const Language language,
    const std::string &store_socket, const std::string &raylet_socket,
    const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
    const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback,
    int num_workers)
    : worker_type_(worker_type),
      language_(language),
      store_socket_(store_socket),
      raylet_socket_(raylet_socket),
      job_id_(job_id),
      io_service_(std::make_shared<boost::asio::io_service>()),
      io_work_(*io_service_),
      num_workers_(num_workers),
      rpc_server_port_(0) {
  RAY_CHECK(num_workers > 0);
  if (worker_type_ == WorkerType::DRIVER) {
    // Driver process can only contain one worker.
    RAY_CHECK(num_workers == 1);
  }

  // Whether to use asio rpc or grpc.
  bool use_asio_rpc = RayConfig::instance().use_asio_rpc_for_worker();

  // Initialize gcs client
  gcs_client_ =
      std::unique_ptr<gcs::RedisGcsClient>(new gcs::RedisGcsClient(gcs_options));
  RAY_CHECK_OK(gcs_client_->Connect(*io_service_));

  InitializeStoreProviders();
  InitializeTaskSubmitters(use_asio_rpc);

  object_interface_ = std::unique_ptr<CoreWorkerObjectInterface>(
      new CoreWorkerObjectInterface(store_providers_, task_submitters_));
  task_interface_ = std::unique_ptr<CoreWorkerTaskInterface>(
      new CoreWorkerTaskInterface(task_submitters_));

  io_thread_ = std::thread(&CoreWorkerProcess::StartIOService, this);

  if (worker_type_ == WorkerType::WORKER) {
    RAY_CHECK(execution_callback != nullptr);
    std::vector<WorkerID> worker_ids;
    for (int i = 0; i < num_workers; i++) {
      const auto worker_id = WorkerID::FromRandom();
      worker_ids.emplace_back(worker_id);
      auto worker = std::make_shared<CoreWorker>(*this, worker_id);
      core_workers_.emplace(worker_id, worker);
    }
    task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
        new CoreWorkerTaskExecutionInterface(core_workers_, store_providers_,
                                             execution_callback, io_service_,
                                             use_asio_rpc));
    rpc_server_port_ = task_execution_interface_->worker_server_->GetPort();
  } else {
    const auto worker_id = ComputeDriverIdFromJob(job_id);
    // This is a driver. Set the thread local `CoreWorker`.
    SetCoreWorker(std::make_shared<CoreWorker>(*this, worker_id));
  }
}

CoreWorkerProcess::~CoreWorkerProcess() {
  gcs_client_->Disconnect();
  io_service_->stop();
  io_thread_.join();
  if (task_execution_interface_) {
    task_execution_interface_->Stop();
  }
  // TODO: join the worker threads.
}

void CoreWorkerProcess::StartIOService() { io_service_->run(); }

void CoreWorkerProcess::InitializeStoreProviders() {
  memory_store_ = std::make_shared<CoreWorkerMemoryStore>();

  store_providers_.emplace(StoreProviderType::LOCAL_PLASMA,
                           CreateStoreProvider(StoreProviderType::LOCAL_PLASMA));
  store_providers_.emplace(StoreProviderType::PLASMA,
                           CreateStoreProvider(StoreProviderType::PLASMA));
  store_providers_.emplace(StoreProviderType::MEMORY,
                           CreateStoreProvider(StoreProviderType::MEMORY));
}

std::unique_ptr<CoreWorkerStoreProvider> CoreWorkerProcess::CreateStoreProvider(
    StoreProviderType type) {
  switch (type) {
  case StoreProviderType::LOCAL_PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerLocalPlasmaStoreProvider(store_socket_));
    break;
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerPlasmaStoreProvider(store_socket_));
    break;
  case StoreProviderType::MEMORY:
    return std::unique_ptr<CoreWorkerStoreProvider>(
        new CoreWorkerMemoryStoreProvider(memory_store_));
    break;
  default:
    // Should never reach here.
    RAY_LOG(FATAL) << "unknown store provider type " << static_cast<int>(type);
    return nullptr;
  }
}

void CoreWorkerProcess::InitializeTaskSubmitters(bool use_asio_rpc) {
  // Add all task submitters.
  task_submitters_.emplace(TaskTransportType::RAYLET,
                           std::unique_ptr<CoreWorkerRayletTaskSubmitter>(
                               new CoreWorkerRayletTaskSubmitter()));

  task_submitters_.emplace(
      TaskTransportType::DIRECT_ACTOR,
      use_asio_rpc ? std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
                         new DirectActorAsioTaskSubmitter(
                             *io_service_, *gcs_client_,
                             CreateStoreProvider(StoreProviderType::MEMORY)))
                   : std::unique_ptr<CoreWorkerDirectActorTaskSubmitter>(
                         new DirectActorGrpcTaskSubmitter(
                             *io_service_, *gcs_client_,
                             CreateStoreProvider(StoreProviderType::MEMORY))));
}

CoreWorker::CoreWorker(CoreWorkerProcess &core_worker_process, const WorkerID &worker_id)
    : core_worker_process_(core_worker_process),
      worker_context_(core_worker_process.GetWorkerType(), worker_id,
                      core_worker_process.GetJobId()),
      raylet_client_(core_worker_process.GetRayletSocket(), worker_id,
                     (worker_context_.GetWorkerType() == ray::WorkerType::WORKER),
                     worker_context_.GetCurrentJobID(), core_worker_process.GetLanguage(),
                     core_worker_process.GetRpcServerPort()),
      main_service_(std::make_shared<boost::asio::io_service>()),
      main_work_(*main_service_) {}

CoreWorker::~CoreWorker() { RAY_IGNORE_EXPR(raylet_client_.Disconnect()); }

}  // namespace ray
