#include "gcs_table_storage.h"
#include <boost/none.hpp>
#include "ray/gcs/callback.h"
#include "ray/util/logging.h"

namespace ray {
namespace gcs {

template <typename Data>
std::string Serialize(const Data &data) {
  std::string value;
  data.SerializeToString(&value);
  return value;
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Put(const JobID &job_id, const KEY &key,
                                 const std::shared_ptr<VALUE> &value,
                                 const StatusCallback &callback) {
  return store_client_.AsyncPut(table_name_, job_id.Binary(), Serialize(key), Serialize(value),
                          callback);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Get(const JobID &job_id, const KEY &key,
                                 const OptionalItemCallback<VALUE> &callback) {
  return store_client_.AsyncGet(table_name_, job_id.Binary(), Serialize(key), callback);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::GetAll(const JobID &job_id,
                                    const MultiItemCallback<VALUE> &callback) {
  return store_client_.AsyncGetAll(table_name_, job_id.Binary(), callback);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Delete(const JobID &job_id, const KEY &key,
                                    const StatusCallback &callback) {
  return store_client_.AsyncDelete(table_name_, job_id.Binary(), Serialize(key), callback);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Delete(const JobID &job_id, const std::vector<KEY> &keys,
                                    const StatusCallback &callback) {
  std::vector<std::string> serialized_keys(keys.size());
  for (KEY key : keys) {
    serialized_keys.push_back(Serialize(key));
  }
  return store_client_.AsyncDelete(table_name_, job_id.Binary(), serialized_keys, callback);
}

template <typename KEY, typename VALUE>
Status GcsTable<KEY, VALUE>::Delete(const JobID &job_id, const StatusCallback &callback) {
  return store_client_.AsyncDeleteByIndex(table_name_, job_id.Binary(), callback);
}

}  // namespace gcs
}  // namespace ray
