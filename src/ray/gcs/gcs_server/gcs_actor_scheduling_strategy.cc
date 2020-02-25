
#include "gcs_actor_scheduling_strategy.h"
#include "gcs_actor_manager.h"
#include "gcs_node_manager.h"

namespace ray {

namespace gcs {

GcsActorSchedulingStrategy::GcsActorSchedulingStrategy(
    std::weak_ptr<GcsNodeManager> &&gcs_node_manager)
    : gcs_node_manager_(std::move(gcs_node_manager)) {}

std::shared_ptr<GcsNode> GcsActorSchedulingStrategy::SelectNode(
    std::shared_ptr<GcsActor> actor) {
  RAY_CHECK(actor);
  if (auto gcs_node_manager = gcs_node_manager_.lock()) {
    auto &alive_nodes = gcs_node_manager->GetAllAliveNodes();
    if (alive_nodes.empty()) {
      return nullptr;
    }
    // TODO(zsl):
    return alive_nodes.begin()->second;
  }
  return nullptr;
}

}  // namespace gcs

}  // namespace ray
