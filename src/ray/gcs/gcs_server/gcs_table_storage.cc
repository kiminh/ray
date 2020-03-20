#include "gcs_table_storage.h"
#include "ray/common/common_protocol.h"
#include "ray/common/constants.h"
#include "ray/common/id.h"
#include "ray/common/status.h"
#include "ray/gcs/callback.h"
#include "ray/gcs/store_client/redis_store_client.h"
#include "ray/protobuf/gcs.pb.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

template <typename Data>
std::string Serialize(const Data &data) {
  std::string value;
  data->SerializeToString(&value);
  return value;
}

template <typename Data>
void Deserialize(std::string value, Data *data) {
  data->ParseFromString(value);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Put(const JobID &job_id, const KEY &key,
                                 const std::shared_ptr<VALUE> &value,
                                 const StatusCallback &callback) {
  RAY_LOG(INFO) << "Putting " << key << " into " << table_name_;
  auto status = store_client_->AsyncPut(table_name_, key.Binary(), job_id.Binary(),
                                        Serialize(value), callback);
  RAY_LOG(INFO) << "Finished putting " << key << " into " << table_name_;
  return status;
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Get(const JobID &job_id, const KEY &key,
                                 const OptionalItemCallback<VALUE> &callback) {
  RAY_LOG(INFO) << "Getting " << key << " from " << table_name_;
  auto on_done = [callback](Status status, const boost::optional<std::string> &result) {
    if (result) {
      VALUE value;
      Deserialize(*result, &value);
      boost::optional<VALUE> data = value;
//      callback(status, boost::none);
      callback(status, data);
    } else {
      callback(status, boost::none);
    }
  };
  auto status = store_client_->AsyncGet(table_name_, key.Binary(), on_done);
  RAY_LOG(INFO) << "Finished getting " << key << " from " << table_name_;
  return status;
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::GetAll(const JobID &job_id,
                                    const MultiItemCallback<VALUE> &callback) {
  RAY_LOG(INFO) << "Getting all " << job_id << " from " << table_name_;
  auto on_done = [callback](
                     Status status, bool has_more,
                     const std::vector<std::pair<std::string, std::string>> &result) {
    // TODO(ffbin): has more
    std::vector<VALUE> data;
    for (auto &pair : result) {
      VALUE value;
      Deserialize(pair.second, &value);
      data.push_back(value);
    }
    callback(status, data);
  };
  auto status = store_client_->AsyncGetAll(table_name_, on_done);
  RAY_LOG(INFO) << "Finished getting all " << job_id << " from " << table_name_;
  return status;
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Delete(const JobID &job_id, const KEY &key,
                                    const StatusCallback &callback) {
  RAY_LOG(INFO) << "Deleting table " << table_name_ << " item, key = " << key
                << ", job id = " << job_id;
  std::string str_key = key.Binary();
  RAY_LOG(INFO) << "Delete str_key size = " << str_key.size();
  auto status = store_client_->AsyncDelete(table_name_, str_key, callback);
  RAY_LOG(INFO) << "Finished deleting table " << table_name_ << " item, key = " << key
                << ", job id = " << job_id;
  return status;
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Delete(const JobID &job_id, const std::vector<KEY> &keys,
                                    const StatusCallback &callback) {
  return Status::OK();
  //  std::vector<std::string> serialized_keys(keys.size());
  //  for (KEY key : keys) {
  //    serialized_keys.push_back(Serialize(key));
  //  }
  //  return store_client_.AsyncDelete(table_name_, job_id.Binary(), serialized_keys,
  //  callback);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Delete(const JobID &job_id, const StatusCallback &callback) {
  RAY_LOG(INFO) << "Deleting table " << table_name_ << " item, job id = " << job_id;
  auto status = store_client_->AsyncDeleteByIndex(table_name_, job_id.Binary(), callback);
  RAY_LOG(INFO) << "Finished deleting table " << table_name_
                << " item, job id = " << job_id;
  return status;
}

template class GcsTable<JobID, JobTableData>;
template class GcsTable<ActorID, ActorTableData>;
template class GcsTable<ActorCheckpointID, ActorCheckpointData>;
template class GcsTable<ActorID, ActorCheckpointIdData>;
template class GcsTable<TaskID, TaskTableData>;
template class GcsTable<TaskID, TaskLeaseData>;
template class GcsTable<TaskID, TaskReconstructionData>;
template class GcsTable<ObjectID, ObjectTableDataList>;
template class GcsTable<ClientID, GcsNodeInfo>;
template class GcsTable<ClientID, ResourceMap>;
template class GcsTable<ClientID, HeartbeatTableData>;
template class GcsTable<ClientID, HeartbeatBatchTableData>;
template class GcsTable<JobID, ErrorTableData>;
template class GcsTable<UniqueID, ProfileTableData>;
template class GcsTable<WorkerID, WorkerFailureData>;

}  // namespace gcs
}  // namespace ray
