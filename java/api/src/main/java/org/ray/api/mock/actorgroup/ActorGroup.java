package org.ray.api.mock.actorgroup;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.api.label.Affinity;

public class ActorGroup implements Iterable<RayActor<?>> {

  List<RayActor<?>> actors;

  /**
   * each actor in the group has its affinities.
   */
  Map<ActorId, List<Affinity>> affinities;

  public List<Affinity> getAffinities(ActorId actorId) {
    return affinities.get(actorId);
  }

  public int size() {
    return actors.size();
  }

  @Override
  public Iterator<RayActor<?>> iterator() {
    return actors.iterator();
  }
}
