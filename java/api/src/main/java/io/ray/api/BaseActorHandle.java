package io.ray.api;

/**
 * A handle to an actor. <p>
 *
 * A handle can be used to invoke a remote actor method.
 */
public interface BaseActorHandle {

  /**
   * Kill the actor immediately. This will cause any outstanding tasks submitted to the actor to
   * fail and the actor to exit in the same way as if it crashed.
   *
   * @param noRestart If set to true, the killed actor will not be restarted anymore.
   */
  void kill(boolean noRestart);
}
