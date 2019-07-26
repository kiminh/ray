#include "ray/core_worker/store_provider/memory_store_provider.h"
#include "ray/common/ray_config.h"
#include "ray/core_worker/context.h"
#include "ray/core_worker/core_worker.h"
#include "ray/core_worker/object_interface.h"
#include <condition_variable>

namespace ray {



/// A RayObject class that automatically releases the backing object
/// when it goes out of scope. This is returned by Get.
class ReferencedRayObject : public RayObject {
 public:
  ~ReferencedRayObject();

  ReferencedRayObject(
      std::shared_ptr<CoreWorkerMemoryStoreProvider::Impl> provider,
      const ObjectID &object_id,
      std::shared_ptr<RayObject> object)
      : RayObject(object->GetData(), object->GetMetadata(), /* should_copy=*/ true),
        provider_(provider),
        object_id_(object_id) {}

 private:
  std::shared_ptr<CoreWorkerMemoryStoreProvider::Impl> provider_;
  const ObjectID object_id_;
};

/// A class that represents an object in memory store provider.
class ObjectEntry {
 public:
  ObjectEntry(const ObjectID &object_id, const RayObject &object);
  
  std::shared_ptr<RayObject> GetObject() const { return object_; }
  int IncreaseRefcnt() { return ++refcnt_; }
  int DecreaseRefcnt() { return --refcnt_; }
  int Refcnt() { return refcnt_; }

  std::shared_ptr<ReferencedRayObject> CreateReferencedObject(
      std::shared_ptr<CoreWorkerMemoryStoreProvider::Impl> provider);
 private:
  const ObjectID object_id_;
  std::shared_ptr<RayObject> object_;
  std::atomic<uint16_t> refcnt_;
};

ObjectEntry::ObjectEntry(
    const ObjectID &object_id, const RayObject &object)
    : object_id_(object_id), refcnt_(0),
      object_(std::make_shared<RayObject>(
          object.GetData(), object.GetMetadata(), /* copy_data */ true)) {}

/// A class that represents a `Get` reuquest.
class GetRequest {
 public:
  GetRequest(const std::vector<ObjectID> &object_ids);
  bool Wait(int64_t timeout_ms);
  void Wait();
  void Set(const ObjectID &object_id, std::shared_ptr<RayObject> buffer);

  /// The object IDs involved in this request. This is used in the reply.
  std::vector<ObjectID> object_ids_;
  /// The object information for the objects in this request.
  std::unordered_map<ObjectID, std::shared_ptr<RayObject>> objects_;
 private:  
  bool is_ready_;
  std::mutex mutex_;
  std::condition_variable cv_;
};

GetRequest::GetRequest(const std::vector<ObjectID> &object_ids)
  : object_ids_(object_ids) {}

bool GetRequest::Wait(int64_t timeout_ms) {
  if (timeout_ms < 0) {
    // Wait forever until the object is ready.
    Wait();
    return true;
  }

  // Wait until the object is ready, or the timeout expires.
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    auto status = cv_.wait_for(lock,
        std::chrono::milliseconds(timeout_ms));
    if (status == std::cv_status::timeout) {
      return false;
    }
  }
  return true;
}

void GetRequest::Wait() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (!is_ready_) {
    cv_.wait(lock);
  }
}

void GetRequest::Set(const ObjectID &object_id, std::shared_ptr<RayObject> object) {
  std::unique_lock<std::mutex> lock(mutex_);
  objects_.emplace(object_id, object);
  if (objects_.size() == object_ids_.size()) {
    is_ready_ = true;
    cv_.notify_all();
  }
}

/// Implementation for CoreWorkerMemoryStoreProvider.
class CoreWorkerMemoryStoreProvider::Impl :
    public std::enable_shared_from_this<CoreWorkerMemoryStoreProvider::Impl> {
 public:
  Impl() {};
  ~Impl() {};

  Status Put(const RayObject &object, const ObjectID &object_id);
  Status Get(const std::vector<ObjectID> &ids, int64_t timeout_ms, 
             std::vector<std::shared_ptr<RayObject>> *results);
  Status Wait(const std::vector<ObjectID> &object_ids,
              int num_objects, int64_t timeout_ms,
              std::vector<bool> *results);
  void Release(const ObjectID &object_id);
  void Delete(const std::vector<ObjectID> &object_ids);
 private:
  Status DeleteObjectImpl(const ObjectID &object_id);

  std::mutex lock_;
  std::unordered_map<ObjectID, std::unique_ptr<ObjectEntry>> objects_;
  std::unordered_map<ObjectID, std::vector<std::shared_ptr<GetRequest>>> object_get_requests_;

};

ReferencedRayObject::~ReferencedRayObject() { provider_->Release(object_id_); }

std::shared_ptr<ReferencedRayObject> ObjectEntry::CreateReferencedObject(
    std::shared_ptr<CoreWorkerMemoryStoreProvider::Impl> provider) {
  // Get a shared_ptr reference RayObject, which will automatically
  // release the refcnt when it destructs, so we increase the refcnt here.       
  IncreaseRefcnt();
  return std::make_shared<ReferencedRayObject>(
      provider, object_id_, object_);
}

Status CoreWorkerMemoryStoreProvider::Impl::Put(const RayObject &object, const ObjectID &object_id) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    return Status::KeyError("object already exists");
  }

  auto entry = std::unique_ptr<ObjectEntry>(
      new ObjectEntry(object_id, object));
  objects_.emplace(object_id, std::move(entry));

  auto object_request_iter = object_get_requests_.find(object_id);
  if (object_request_iter != object_get_requests_.end()) {
    auto& get_requests = object_request_iter->second;
    for (auto &get_req : get_requests) {
      auto& object_entry = objects_[object_id];
      get_req->Set(object_id,
          object_entry->CreateReferencedObject(shared_from_this()));
    }
  }
  return Status::OK();
}

Status CoreWorkerMemoryStoreProvider::Impl::Get(
    const std::vector<ObjectID> &object_ids, int64_t timeout_ms, 
    std::vector<std::shared_ptr<RayObject>> *results) {
  (*results).resize(object_ids.size(), nullptr);
  std::vector<ObjectID> remaining_ids;

  std::shared_ptr<GetRequest> get_request;

  {
    std::unique_lock<std::mutex> lock(lock_);
    // Check for existing objects and see if this get request can be fullfilled.
    for (int i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      auto iter = objects_.find(object_id);
      if (iter != objects_.end()) {
        (*results)[i] = iter->second->CreateReferencedObject(shared_from_this());
      } else {
        remaining_ids.emplace_back(object_id);
      }
    }

    // Return if all the objects are obtained.
    if (remaining_ids.empty()) {
      return Status::OK();
    }

    // Otherwise, create a GetRequest to track remaining objects.
    get_request = std::make_shared<GetRequest>(remaining_ids);
    for (const auto &object_id : remaining_ids) {
      object_get_requests_[object_id].push_back(get_request);
    }
  }

  // Wait for remaining objects (or timeout).
  get_request->Wait(timeout_ms);

  {
    std::unique_lock<std::mutex> lock(lock_);
    // Populate results.
    for (int i = 0; i < object_ids.size(); i++) {
      const auto &object_id = object_ids[i];
      if ((*results)[i] == nullptr) {
        auto iter = get_request->objects_.find(object_id);
        if (iter != get_request->objects_.end()) {
          (*results)[i] = iter->second;
        }
      }
    }

    // Remove get rquest.
    for (ObjectID& object_id : get_request->object_ids_) {
      auto object_request_iter = object_get_requests_.find(object_id);
      if (object_request_iter != object_get_requests_.end()) {
        auto& get_requests = object_request_iter->second;
        // Erase get_req from the vector.
        auto it = std::find(get_requests.begin(), get_requests.end(), get_request);
        if (it != get_requests.end()) {
          get_requests.erase(it);
          // If the vector is empty, remove the object ID from the map.
          if (get_requests.empty()) {
            object_get_requests_.erase(object_request_iter);
          }
        }
      }
    }
  }

  return Status::OK();
}


Status CoreWorkerMemoryStoreProvider::Impl::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           std::vector<bool> *results) {
  // TODO
  return Status::OK();
}

void CoreWorkerMemoryStoreProvider::Impl::Release(const ObjectID &object_id) {
  std::unique_lock<std::mutex> lock(lock_);
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    if (iter->second->DecreaseRefcnt() == 0) {
      // TODO(zhijunfu): do we want to preserve it here?
      objects_.erase(iter);
    }
  }
}

void CoreWorkerMemoryStoreProvider::Impl::Delete(const std::vector<ObjectID> &object_ids) {
  std::unique_lock<std::mutex> lock(lock_);
  for (const auto &object_id : object_ids) {
    RAY_UNUSED(DeleteObjectImpl(object_id));
  }
}

Status CoreWorkerMemoryStoreProvider::Impl::DeleteObjectImpl(const ObjectID &object_id) {
  // Note that this function doesn't take a lock.
  auto iter = objects_.find(object_id);
  if (iter != objects_.end()) {
    if (iter->second->Refcnt() == 0) {
      // TODO(zhijunfu): do we want to directly delete here, or just mark a flag
      // and actually do the deletion when refcnt drops to 0? It's ok just to delete
      // as the data validity is ensured by shared_ptr.
      objects_.erase(iter);
      return Status::OK();
    }
    return Status::Invalid("reference count is not zero");
  }
  return Status::Invalid("object not exists");
}

//
// CoreWorkerMemoryStoreProvider functions
//
CoreWorkerMemoryStoreProvider::CoreWorkerMemoryStoreProvider()
    : impl_(std::make_shared<CoreWorkerMemoryStoreProvider::Impl>()) {}

Status CoreWorkerMemoryStoreProvider::Put(const RayObject &object,
                                          const ObjectID &object_id) {
  return impl_->Put(object, object_id);
}

Status CoreWorkerMemoryStoreProvider::Get(const std::vector<ObjectID> &ids,
                                          int64_t timeout_ms, const TaskID &task_id,
                                          std::vector<std::shared_ptr<RayObject>> *results) {
  return impl_->Get(ids, timeout_ms, results);
}

Status CoreWorkerMemoryStoreProvider::Wait(const std::vector<ObjectID> &object_ids,
                                           int num_objects, int64_t timeout_ms,
                                           const TaskID &task_id,
                                           std::vector<bool> *results) {
  return impl_->Wait(object_ids, num_objects, timeout_ms, results);
}

Status CoreWorkerMemoryStoreProvider::Delete(const std::vector<ObjectID> &object_ids,
                                             bool local_only,
                                             bool delete_creating_tasks) {
  impl_->Delete(object_ids);
  return Status::OK();
}

}  // namespace ray
