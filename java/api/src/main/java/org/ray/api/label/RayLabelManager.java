package org.ray.api.label;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.ray.api.RayActor;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ActorId;
import org.ray.api.mock.actorgroup.ActorGroup;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.runtimecontext.NodeInfo;

/**
 * label manager allows user to manage labels on ray nodes and ray actors.
 */
public interface RayLabelManager {

  /**
   * distribute labels to ray nodes.
   * user can set labels to ray nodes without knowing any details about ray nodes.
   * eg:
   * labels = {k1: v1}, affinity1 = {EQ, k2, v2}, affinity2 = {op: NOT_IN, k3, [v31, v32]}.
   * And there are 3 ray nodes with labels:
   * node1 {k2: v2}, node2 {k2: v0, k3: v30}, node3: {k2: v2, k3: v31}
   *
   * when user distribute `labels` with `affinity1` and `affinity2` to those 3 nodes (user do not concern ray node),
   * Ray core will distribute the labels to node1 because:
   * node1 meet the requirements both affinity1 and affinity2.
   * node2 not meet the requirement of affinity1.
   * node3 not meet the requirement of affinity2.
   *
   * @param labels
   * @param numMinNodes
   * @param numMaxNodes
   * @param affinities
   * @return
   */
  boolean distributeLabelsToRayNodes(Map<String, String> labels, int numMinNodes, int numMaxNodes,
      Affinity... affinities);

  /**
   * matching ray nodes for the given affinities (usually required by an actor)
   *
   * FIXME: actor creation is handled in c++ core, how to execute this method.
   * @param affinities
   * @return
   * @see org.ray.api.runtime.RayRuntime#createActor(RayFunc, Object[], ActorCreationOptions)
   */
  List<NodeInfo> matching(List<Affinity> affinities);

  /**
   * matching ray nodes for the given creation options (usually required by an actor)
   *
   * @param options
   * @return
   */
  default List<NodeInfo> matching(ActorCreationOptions options) {
    return matching(options.affinities);
  }

  default Map<ActorId, List<NodeInfo>> matching(Map<ActorId, List<Affinity>> actorWithAffinities) {
    Map<ActorId, List<NodeInfo>> result = new HashMap<>();
    for (Entry<ActorId, List<Affinity>> entry : actorWithAffinities.entrySet()) {
      result.put(entry.getKey(), matching(entry.getValue()));
    }
    return result;
  }

  /**
   * matching for a group of actors
   * @param actorGroup
   * @return
   */
  default Map<ActorId, List<NodeInfo>> matching(ActorGroup actorGroup) {
    Map<ActorId, List<NodeInfo>> result = new HashMap<>();
    for (RayActor<?> actor : actorGroup) {
      List<NodeInfo> actorMatchResult = matching(actorGroup.getAffinities(actor.getId()));
      result.put(actor.getId(), actorMatchResult);
    }
    return result;
  }

}
