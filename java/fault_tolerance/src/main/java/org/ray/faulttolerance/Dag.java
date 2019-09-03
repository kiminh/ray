package org.ray.faulttolerance;

import java.util.List;
import org.ray.api.id.ActorId;

/**
 * A directed acyclic graph that consists of many actors.
 */
public final class Dag {

  /**
   * A node in the DAG.
   */
  public static class Node {

    public final ActorId id;

    public final List<Node> upstreamNodes;

    public final List<Node> downstreamNodes;

    public Node(ActorId id, List<Node> upstreamNodes,
        List<Node> downstreamNodes) {
      this.id = id;
      this.upstreamNodes = upstreamNodes;
      this.downstreamNodes = downstreamNodes;
    }
  }

  public final List<Node> nodes;

  public Dag(List<Node> nodes) {
    this.nodes = nodes;
  }
}
