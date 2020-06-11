package io.ray.api.call;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.RayFuncR;
import io.ray.api.options.ActorCreationOptions;

/**
 * A helper to create java actor.
 *
 * @param <A> The type of the concrete actor class.
 */
public class ActorCreator<A> extends BaseActorCreator<ActorCreator<A>> {
  private final RayFuncR<A> func;
  private final Object[] args;

  public ActorCreator(RayFuncR<A> func, Object[] args) {
    this.func = func;
    this.args = args;
  }

  /**
   * @see ActorCreationOptions.Builder#setJvmOptions(java.lang.String)
   */
  public ActorCreator<A> setJvmOptions(String jvmOptions) {
    builder.setJvmOptions(jvmOptions);
    return this;
  }

  /**
   * Create a java actor remotely and return a handle to the created actor.
   *
   * @return a handle to the created java actor.
   */
  public ActorHandle<A> remote() {
    return Ray.internal().createActor(func, args, buildOptions());
  }

}
