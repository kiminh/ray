#ifndef RAY_GCS_ACTOR_SCHEDULING_STRATEGY_H
#define RAY_GCS_ACTOR_SCHEDULING_STRATEGY_H

#include <memory>

namespace ray {
namespace gcs {
class GcsActor;
class GcsNode;
class GcsNodeManager;
class GcsActorSchedulingStrategy {
 public:
  explicit GcsActorSchedulingStrategy(std::weak_ptr<GcsNodeManager> &&gcs_node_manager);
  std::shared_ptr<GcsNode> SelectNode(std::shared_ptr<GcsActor> actor);

 private:
  std::weak_ptr<GcsNodeManager> gcs_node_manager_;
};
}  // namespace gcs
}  // namespace ray

#endif  // RAY_GCS_ACTOR_SCHEDULING_STRATEGY_H
