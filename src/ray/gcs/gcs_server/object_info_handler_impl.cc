#include "object_info_handler_impl.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultObjectInfoHandler::HandleGetObjectLocations(
    const GetObjectLocationsRequest &request, GetObjectLocationsReply *reply,
    SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  RAY_LOG(DEBUG) << "Getting object locations, object id = " << object_id;

  auto on_done = [reply, object_id, send_reply_callback](
                     Status status,
                     const boost::optional<rpc::ObjectTableDataList> &result) {
    if (status.ok()) {
      if (result) {
        for (int index = 0; index < result->items_size(); ++index) {
          auto item = result->items(index);
          reply->add_object_table_data_list()->CopyFrom(item);
        }
      }
      RAY_LOG(DEBUG) << "Finished getting object locations, object id = " << object_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get object locations: " << status.ToString()
                     << ", object id = " << object_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->ObjectTable().Get(object_id.TaskId().JobId(),
                                                        object_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultObjectInfoHandler::HandleAddObjectLocation(
    const AddObjectLocationRequest &request, AddObjectLocationReply *reply,
    SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Adding object location, object id = " << object_id
                 << ", node id = " << node_id;

  auto on_done = [this, object_id, node_id, send_reply_callback](
                     Status status,
                     const boost::optional<rpc::ObjectTableDataList> &result) {
    auto on_done = [this, object_id, node_id, send_reply_callback](Status status) {
      if (status.ok()) {
        RAY_LOG(DEBUG) << "Finished adding object location, object id = " << object_id
                       << ", node id = " << node_id;
        ObjectTableData object_table_data;
        object_table_data.set_manager(node_id.Binary());
        RAY_CHECK_OK(object_pub_.Publish(object_id, object_table_data,
                                         GcsChangeMode::APPEND_OR_ADD, nullptr));
      } else {
        RAY_LOG(ERROR) << "Failed to add object location: " << status.ToString()
                       << ", object id = " << object_id << ", node id = " << node_id;
      }
      send_reply_callback(status, nullptr, nullptr);
    };

    if (status.ok()) {
      std::shared_ptr<rpc::ObjectTableDataList> data =
          result ? std::make_shared<rpc::ObjectTableDataList>(*result)
                 : std::make_shared<rpc::ObjectTableDataList>();
      ObjectTableData object_table_data;
      object_table_data.set_manager(node_id.Binary());
      data->add_items()->CopyFrom(object_table_data);
      status = gcs_table_storage_->ObjectTable().Put(object_id.TaskId().JobId(),
                                                     object_id, data, on_done);
      if (!status.ok()) {
        on_done(status);
      }
    } else {
      on_done(status);
    }
  };

  Status status = gcs_table_storage_->ObjectTable().Get(object_id.TaskId().JobId(),
                                                        object_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultObjectInfoHandler::HandleRemoveObjectLocation(
    const RemoveObjectLocationRequest &request, RemoveObjectLocationReply *reply,
    SendReplyCallback send_reply_callback) {
  ObjectID object_id = ObjectID::FromBinary(request.object_id());
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Removing object location, object id = " << object_id
                 << ", node id = " << node_id;

  auto on_done = [this, object_id, node_id, send_reply_callback](
                     Status status,
                     const boost::optional<rpc::ObjectTableDataList> &result) {
    auto on_done = [this, object_id, node_id, send_reply_callback](Status status) {
      if (status.ok()) {
        RAY_LOG(DEBUG) << "Finished removing object location, object id = " << object_id
                       << ", node id = " << node_id;
        ObjectTableData object_table_data;
        object_table_data.set_manager(node_id.Binary());
        RAY_CHECK_OK(object_pub_.Publish(object_id, object_table_data,
                                         GcsChangeMode::REMOVE, nullptr));
      } else {
        RAY_LOG(ERROR) << "Failed to remove object location: " << status.ToString()
                       << ", object id = " << object_id << ", node id = " << node_id;
      }
      send_reply_callback(status, nullptr, nullptr);
    };

    if (status.ok()) {
      std::vector<ObjectTableData> objects = VectorFromProtobuf(result->items());
      objects.erase(std::remove_if(objects.begin(), objects.end(),
                                   [node_id](const ObjectTableData &data) {
                                     return data.manager() == node_id.Binary();
                                   }),
                    objects.end());

      std::shared_ptr<rpc::ObjectTableDataList> data =
          std::make_shared<rpc::ObjectTableDataList>();
      for (ObjectTableData object : objects) {
        data->add_items()->CopyFrom(object);
      }
      status = gcs_table_storage_->ObjectTable().Put(object_id.TaskId().JobId(),
                                                     object_id, data, on_done);
      if (!status.ok()) {
        on_done(status);
      }
    } else {
      on_done(status);
    }
  };

  Status status = gcs_table_storage_->ObjectTable().Get(object_id.TaskId().JobId(),
                                                        object_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

}  // namespace rpc
}  // namespace ray
