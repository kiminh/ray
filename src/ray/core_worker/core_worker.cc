#include <chrono>

#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
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
    const std::unordered_map<std::string, std::string> &static_worker_info,
    const std::string &store_socket, const std::string &raylet_socket,
    const JobID &job_id, const gcs::GcsClientOptions &gcs_options,
    const CoreWorkerTaskExecutionInterface::TaskExecutor &execution_callback,
    int num_workers)
    : worker_type_(worker_type),
      language_(language),
      static_worker_info_(static_worker_info),
      store_socket_(store_socket),
      raylet_socket_(raylet_socket),
      job_id_(job_id),
      io_service_(std::make_shared<boost::asio::io_service>()),
      io_work_(*io_service_) {
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
      RegisterWorker(worker_id);
    }
    task_execution_interface_ = std::unique_ptr<CoreWorkerTaskExecutionInterface>(
        new CoreWorkerTaskExecutionInterface(core_workers_, store_providers_,
                                             execution_callback, io_service_,
                                             use_asio_rpc));
  } else {
    const auto worker_id = ComputeDriverIdFromJob(job_id);
    auto worker = std::make_shared<CoreWorker>(*this, worker_id);
    worker->ConnectToStore();
    worker->ConnectToRaylet(/*rpc_server_port=*/0);
    core_workers_.emplace(worker_id, worker);
    RegisterWorker(worker_id);
    // This is a driver. Set the thread local `CoreWorker`.
    SetCoreWorker(worker);
  }
}

CoreWorkerProcess::~CoreWorkerProcess() {
  gcs_client_->Disconnect();
  io_service_->stop();
  io_thread_.join();
  if (task_execution_interface_) {
    task_execution_interface_->Stop();
  }
  if (worker_type_ == WorkerType::DRIVER) {
    SetCoreWorker(nullptr);
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
        new CoreWorkerLocalPlasmaStoreProvider());
    break;
  case StoreProviderType::PLASMA:
    return std::unique_ptr<CoreWorkerStoreProvider>(new CoreWorkerPlasmaStoreProvider());
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

void CoreWorkerProcess::RegisterWorker(const WorkerID &worker_id) {
  std::unordered_map<std::string, std::string> worker_info;
  auto it = static_worker_info_.find("node_ip_address");
  if (it != static_worker_info_.end()) {
    worker_info.emplace("node_ip_address", it->second);
  }
  worker_info.emplace("plasma_store_socket", store_socket_);
  worker_info.emplace("raylet_socket", raylet_socket_);
  if (worker_type_ == WorkerType::DRIVER) {
    auto start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                          std::chrono::system_clock::now().time_since_epoch())
                          .count();
    worker_info.emplace("driver_id", worker_id.Binary());
    worker_info.emplace("start_time", std::to_string(start_time));
    it = static_worker_info_.find("name");
    if (it != static_worker_info_.end()) {
      worker_info.emplace("name", it->second);
    }
  }

  std::vector<std::string> args;
  args.emplace_back("HMSET");
  if (worker_type_ == WorkerType::DRIVER) {
    args.emplace_back("Drivers:" + worker_id.Binary());
  } else {
    args.emplace_back("Workers:" + worker_id.Binary());
  }
  for (const auto &entry : worker_info) {
    args.push_back(entry.first);
    args.push_back(entry.second);
  }
  RAY_CHECK_OK(gcs_client_->primary_context()->RunArgvAsync(args));
}

CoreWorker::CoreWorker(CoreWorkerProcess &core_worker_process, const WorkerID &worker_id)
    : core_worker_process_(core_worker_process),
      worker_context_(core_worker_process.GetWorkerType(), worker_id,
                      core_worker_process.GetJobId()),
      raylet_client_(),
      main_service_(std::make_shared<boost::asio::io_service>()),
      main_work_(*main_service_) {}

void CoreWorker::ConnectToStore() {
  RAY_ARROW_CHECK_OK(store_client_.Connect(core_worker_process_.GetStoreSocket()));
}

void CoreWorker::ConnectToRaylet(int rpc_server_port) {
  RAY_CHECK(!raylet_client_);
  raylet_client_ = std::unique_ptr<RayletClient>(
      new RayletClient(core_worker_process_.GetRayletSocket(), GetWorkerID(),
                       (worker_context_.GetWorkerType() == ray::WorkerType::WORKER),
                       worker_context_.GetCurrentJobID(),
                       core_worker_process_.GetLanguage(), rpc_server_port));
}

CoreWorker::~CoreWorker() { RAY_IGNORE_EXPR(raylet_client_->Disconnect()); }

}  // namespace ray
