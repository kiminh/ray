// Copyright 2017 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef RAY_GCS_PLACEMRNT_GROUP_SCHEDULER_H
#define RAY_GCS_PLACEMRNT_GROUP_SCHEDULER_H

#include <ray/common/id.h>
#include <ray/gcs/accessor.h>
#include <ray/protobuf/gcs_service.pb.h>
#include <ray/raylet/raylet_client.h>
#include <ray/rpc/node_manager/node_manager_client.h>
#include <ray/rpc/worker/core_worker_client.h>
#include <queue>
#include <tuple>

#include "absl/container/flat_hash_map.h"
#include "absl/container/flat_hash_set.h"
#include "gcs_node_manager.h"
#include "gcs_table_storage.h"

namespace ray {
namespace gcs {

using LeaseResourceClientFactoryFn =
    std::function<std::shared_ptr<ResourceLeaseInterface>(const rpc::Address &address)>;

typedef std::function<void(const Status &, const rpc::RequestResourceLeaseReply &)>
    LeaseResourceCallback;

class GcsPlacementGroup;

class GcsPlacementGroupSchedulerInterface {
 public:
  /// Schedule the specified placement_group.
  ///
  /// \param placement_group to be scheduled.
  virtual void Schedule(std::shared_ptr<GcsPlacementGroup> placement_group) = 0;

  virtual ~GcsPlacementGroupSchedulerInterface() {}
};

class GcsScheduleStrategy {
 public:
  virtual std::unordered_map<BundleID, ClientID> Schedule(
      std::vector<ray::BundleSpecification> &bundles,
      const GcsNodeManager &node_manager) = 0;
};

class GcsPackStrategy : public GcsScheduleStrategy {
 public:
  std::unordered_map<BundleID, ClientID> Schedule(
      std::vector<ray::BundleSpecification> &bundles,
      const GcsNodeManager &node_manager) override;
};

class GcsSpreadStrategy : public GcsScheduleStrategy {
 public:
  std::unordered_map<BundleID, ClientID> Schedule(
      std::vector<ray::BundleSpecification> &bundles,
      const GcsNodeManager &node_manager) override;
};

/// GcsPlacementGroupScheduler is responsible for scheduling placement_groups registered
/// to GcsPlacementGroupManager. This class is not thread-safe.
class GcsPlacementGroupScheduler : public GcsPlacementGroupSchedulerInterface {
 public:
  /// Create a GcsPlacementGroupScheduler
  ///
  /// \param io_context The main event loop.
  /// \param placement_group_info_accessor Used to flush placement_group info to storage.
  /// \param gcs_node_manager The node manager which is used when scheduling.
  /// \param schedule_failure_handler Invoked when there are no available nodes to
  /// schedule placement_groups.
  /// \param schedule_success_handler Invoked when placement_groups are created on the
  /// worker successfully.
  explicit GcsPlacementGroupScheduler(
      boost::asio::io_context &io_context,
      std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage,
      const GcsNodeManager &gcs_node_manager,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler,
      std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler,
      LeaseResourceClientFactoryFn lease_client_factory = nullptr);
  virtual ~GcsPlacementGroupScheduler() = default;

  /// Schedule the specified placement_group.
  /// If there is no available nodes then the `schedule_failed_handler_` will be
  /// triggered, otherwise the placement_group will be scheduled until succeed or
  /// canceled.
  ///
  /// \param placement_group to be scheduled.
  void Schedule(std::shared_ptr<GcsPlacementGroup> placement_group) override;

//   std::deque<std::tuple<BundleSpecification, std::shared_ptr<rpc::GcsNodeInfo>,
//                         LeaseResourceCallback>>

  /// Lease resource from the specified node for the specified bundle.
  void LeaseResourceFromNode();

  /// return resource for the specified node for the specified bundle.
  ///
  /// \param bundle A description of the bundle to return.
  /// \param node The node that the worker will be returned for.
  void RetureReourceForNode(BundleSpecification bundle_spec,
                            std::shared_ptr<ray::rpc::GcsNodeInfo> node);

  /// Get an existing lease client or connect a new one.
  std::shared_ptr<ResourceLeaseInterface> GetOrConnectLeaseClient(
      const rpc::Address &raylet_address);

 protected:
  /// The io loop that is used to delay execution of tasks (e.g.,
  /// execute_after).
  boost::asio::io_context &io_context_;
  /// Used to update placement group information upon creation, deletion, etc.
  std::shared_ptr<gcs::GcsTableStorage> gcs_table_storage_;
  /// Reference of GcsNodeManager.
  const GcsNodeManager &gcs_node_manager_;
  /// The handler to handle the scheduling failures.
  std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_failure_handler_;
  /// The handler to handle the successful scheduling.
  std::function<void(std::shared_ptr<GcsPlacementGroup>)> schedule_success_handler_;
  /// The cached node clients which are used to communicate with raylet to lease workers.
  absl::flat_hash_map<ClientID, std::shared_ptr<ResourceLeaseInterface>>
      remote_lease_clients_;
  /// Factory for producing new clients to request leases from remote nodes.
  LeaseResourceClientFactoryFn lease_client_factory_;

  /// Map from node ID to the set of actors for whom we are trying to acquire a lease from
  /// that node. This is needed so that we can retry lease requests from the node until we
  /// receive a reply or the node is removed.
  absl::flat_hash_map<ClientID, absl::flat_hash_set<BundleID>>
      node_to_bundles_when_leasing_;

  /// The queue which is used to store the whole LeaseResourceFromNode
  std::deque<std::tuple<BundleSpecification, std::shared_ptr<rpc::GcsNodeInfo>,
                        LeaseResourceCallback>>
      lease_resource_queue_;

  std::vector<std::shared_ptr<GcsScheduleStrategy>> scheduler;
};

}  // namespace gcs
}  // namespace ray

#endif