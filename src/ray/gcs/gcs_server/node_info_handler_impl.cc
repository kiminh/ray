#include "node_info_handler_impl.h"
#include "ray/common/grpc_util.h"
#include "ray/util/logging.h"

namespace ray {
namespace rpc {

void DefaultNodeInfoHandler::HandleRegisterNode(
    const rpc::RegisterNodeRequest &request, rpc::RegisterNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  GcsNodeInfo node_info = request.node_info();
  ClientID node_id = ClientID::FromBinary(node_info.node_id());
  RAY_LOG(DEBUG) << "Registering node info, node id = " << node_id;

  auto on_done = [this, node_id, node_info, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to register node info: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(DEBUG) << "Finished registering node info, node id = " << node_id;
      RAY_CHECK_OK(node_pub_.Publish(JobID::Nil(), ClientID::Nil(), node_id, node_info,
          GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->NodeTable().Put(
      JobID::Nil(), node_id, std::make_shared<GcsNodeInfo>(node_info), on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::HandleUnregisterNode(
    const rpc::UnregisterNodeRequest &request, rpc::UnregisterNodeReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Unregistering node info, node id = " << node_id;

  auto on_done = [this, node_id, send_reply_callback](
      Status status, const boost::optional<rpc::GcsNodeInfo> &result) {
    auto on_done = [this, node_id, result, send_reply_callback](Status status) {
      if (!status.ok()) {
        RAY_LOG(ERROR) << "Failed to unregister node info: " << status.ToString()
                       << ", node id = " << node_id;
      } else {
        RAY_LOG(DEBUG) << "Finished unregistering node info, node id = " << node_id;
        RAY_CHECK_OK(node_pub_.Publish(JobID::Nil(), ClientID::Nil(), node_id, *result,
                                       GcsChangeMode::REMOVE, nullptr));
      }
      send_reply_callback(status, nullptr, nullptr);
    };

    status = gcs_table_storage_->NodeTable().Delete(JobID::Nil(), node_id, on_done);
    if (!status.ok()) {
      on_done(status);
    }
  };

  Status status = gcs_table_storage_->NodeTable().Get(JobID::Nil(), node_id, on_done);
  if (!status.ok()) {
    RAY_LOG(ERROR) << "Failed to get node info: " << status.ToString()
                   << ", node id = " << node_id;
    send_reply_callback(status, nullptr, nullptr);
  }
}

void DefaultNodeInfoHandler::HandleGetAllNodeInfo(
    const rpc::GetAllNodeInfoRequest &request, rpc::GetAllNodeInfoReply *reply,
    rpc::SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Getting all nodes info.";
  auto on_done = [reply, send_reply_callback](
                     Status status, const std::vector<rpc::GcsNodeInfo> &result) {
    if (status.ok()) {
      for (const rpc::GcsNodeInfo &node_info : result) {
        reply->add_node_info_list()->CopyFrom(node_info);
      }
      RAY_LOG(DEBUG) << "Finished getting all node info.";
    } else {
      RAY_LOG(ERROR) << "Failed to get all nodes info: " << status.ToString();
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->NodeTable().GetAll(JobID::Nil(), on_done);
  if (!status.ok()) {
    on_done(status, std::vector<rpc::GcsNodeInfo>());
  }
}

void DefaultNodeInfoHandler::HandleReportHeartbeat(
    const ReportHeartbeatRequest &request, ReportHeartbeatReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.heartbeat().client_id());
  RAY_LOG(DEBUG) << "Reporting heartbeat, node id = " << node_id;

  auto heartbeat_data = std::make_shared<rpc::HeartbeatTableData>();
  heartbeat_data->CopyFrom(request.heartbeat());

  auto on_done = [this, node_id, heartbeat_data, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report heartbeat: " << status.ToString()
                     << ", node id = " << node_id;
    } else {
      RAY_LOG(DEBUG) << "Finished reporting heartbeat, node id = " << node_id;
      RAY_CHECK_OK(heartbeat_pub_.Publish(JobID::Nil(), ClientID::Nil(), node_id, *heartbeat_data,
          GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->HeartbeatTable().Put(JobID::Nil(), node_id,
                                                           heartbeat_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::HandleReportBatchHeartbeat(
    const ReportBatchHeartbeatRequest &request, ReportBatchHeartbeatReply *reply,
    SendReplyCallback send_reply_callback) {
  RAY_LOG(DEBUG) << "Reporting batch heartbeat, batch size = "
                 << request.heartbeat_batch().batch_size();

  auto heartbeat_batch_data = std::make_shared<rpc::HeartbeatBatchTableData>();
  heartbeat_batch_data->CopyFrom(request.heartbeat_batch());
  auto on_done = [this, &request, heartbeat_batch_data, send_reply_callback](Status status) {
    if (!status.ok()) {
      RAY_LOG(ERROR) << "Failed to report batch heartbeat: " << status.ToString()
                     << ", batch size = " << request.heartbeat_batch().batch_size();
    } else {
      RAY_LOG(DEBUG) << "Finished reporting batch heartbeat, batch size = "
                     << request.heartbeat_batch().batch_size();
      RAY_CHECK_OK(heartbeat_batch_pub_.Publish(JobID::Nil(), ClientID::Nil(), ClientID::Nil(),
          *heartbeat_batch_data, GcsChangeMode::APPEND_OR_ADD, nullptr));
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status = gcs_table_storage_->HeartbeatBatchTable().Put(
      JobID::Nil(), ClientID::Nil(), heartbeat_batch_data, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

void DefaultNodeInfoHandler::HandleGetResources(const GetResourcesRequest &request,
                                                GetResourcesReply *reply,
                                                SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(DEBUG) << "Getting node resources, node id = " << node_id;

  auto on_done = [node_id, reply, send_reply_callback](
                     Status status, const boost::optional<rpc::ResourceMap> &result) {
    if (status.ok()) {
      if (result) {
        reply->mutable_resources()->insert(result->items().begin(),
                                           result->items().end());
      }
      RAY_LOG(DEBUG) << "Finished getting node resources, node id = " << node_id;
    } else {
      RAY_LOG(ERROR) << "Failed to get node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
    send_reply_callback(status, nullptr, nullptr);
  };

  Status status =
      gcs_table_storage_->NodeResourceTable().Get(JobID::Nil(), node_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultNodeInfoHandler::HandleUpdateResources(
    const UpdateResourcesRequest &request, UpdateResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  RAY_LOG(INFO) << "Updating node resources, node id = " << node_id;
  auto on_done = [this, node_id, request, send_reply_callback](
                     Status status, const boost::optional<rpc::ResourceMap> &result) {
    if (status.ok()) {
      // Get resources and update
      std::unordered_map<std::string, rpc::ResourceTableData> resource_map;
      if (result) {
        resource_map = MapFromProtobuf(result->items());
      }

      for (auto &resource : request.resources()) {
        resource_map[resource.first] = resource.second;
      }

      auto callback = [this, node_id, request]() {
        rpc::ResourceMap resource;
        resource.mutable_items()->insert(request.resources().begin(), request.resources().end());
        RAY_CHECK_OK(node_resource_pub_.Publish(JobID::Nil(), ClientID::Nil(), node_id,
                                                resource, GcsChangeMode::APPEND_OR_ADD, nullptr));
      };

      UpdateResource(node_id, resource_map, send_reply_callback, callback);
      RAY_LOG(INFO) << "Finished updating node resources, node id = " << node_id;
    } else {
      send_reply_callback(status, nullptr, nullptr);
      RAY_LOG(ERROR) << "Failed to get node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
  };

  Status status =
      gcs_table_storage_->NodeResourceTable().Get(JobID::Nil(), node_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultNodeInfoHandler::HandleDeleteResources(
    const DeleteResourcesRequest &request, DeleteResourcesReply *reply,
    SendReplyCallback send_reply_callback) {
  ClientID node_id = ClientID::FromBinary(request.node_id());
  auto resource_names = VectorFromProtobuf(request.resource_name_list());
  RAY_LOG(DEBUG) << "Deleting node resources, node id = " << node_id;
  auto on_done = [this, node_id, resource_names, send_reply_callback](
                     Status status, const boost::optional<rpc::ResourceMap> &result) {
    if (status.ok()) {
      // Get resources and update
      auto resource_map = MapFromProtobuf(result->items());
      for (auto resource_name : resource_names) {
        resource_map.erase(resource_name);
      }

      auto callback = [this, node_id, result]() {
        RAY_CHECK_OK(node_resource_pub_.Publish(JobID::Nil(), ClientID::Nil(), node_id,
                                                *result, GcsChangeMode::REMOVE, nullptr));
      };

      UpdateResource(node_id, resource_map, send_reply_callback, callback);
      RAY_LOG(DEBUG) << "Finished deleting node resources, node id = " << node_id;
    } else {
      send_reply_callback(status, nullptr, nullptr);
      RAY_LOG(ERROR) << "Failed to delete node resources: " << status.ToString()
                     << ", node id = " << node_id;
    }
  };

  Status status =
      gcs_table_storage_->NodeResourceTable().Get(JobID::Nil(), node_id, on_done);
  if (!status.ok()) {
    on_done(status, boost::none);
  }
}

void DefaultNodeInfoHandler::UpdateResource(
    const ClientID &node_id,
    const std::unordered_map<std::string, rpc::ResourceTableData> &resource_map,
    SendReplyCallback send_reply_callback,
    std::function<void()> callback) {
  std::shared_ptr<rpc::ResourceMap> resources = std::make_shared<rpc::ResourceMap>();
  resources->mutable_items()->insert(resource_map.begin(), resource_map.end());

  auto on_done = [resources, send_reply_callback, callback](Status status) {
    send_reply_callback(status, nullptr, nullptr);
    callback();
  };
  Status status = gcs_table_storage_->NodeResourceTable().Put(JobID::Nil(), node_id,
                                                              resources, on_done);
  if (!status.ok()) {
    on_done(status);
  }
}

}  // namespace rpc
}  // namespace ray
