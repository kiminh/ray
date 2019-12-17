package org.ray.streaming.runtime.rpc.call;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.options.ActorCreationOptions;
import org.slf4j.Logger;

import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.JobWorkerContext;

public class RemoteCallWorker extends RemoteCallBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  public static RayActor createWorker(byte[] confBytes, ActorCreationOptions options) {
    return Ray.createActor(JobWorker::new, confBytes, options);
  }

  public static RayObject<Boolean> initWorker(RayActor actor, JobWorkerContext ctx) {
    LOG.info("Call worker to init, actor: {}, context: {}.", actor.getId(), ctx);
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = Ray.call(JobWorker::init, actor, ctx);
    }

    LOG.info("Finish call worker to init.");
    return result;
  }

  public static RayObject<Boolean> startWorker(RayActor actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = Ray.call(JobWorker::start, actor);
    }

    LOG.info("Finish call worker to start.");
    return result;
  }

  public static RayObject<Boolean> destroyWorker(RayActor actor) {
    LOG.info("Call worker to destroy, actor is {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = Ray.call(JobWorker::destroy, actor);
    }

    LOG.info("Finish call worker to destroy.");
    return result;
  }
}
