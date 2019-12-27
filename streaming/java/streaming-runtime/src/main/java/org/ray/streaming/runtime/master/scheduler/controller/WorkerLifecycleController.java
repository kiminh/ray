package org.ray.streaming.runtime.master.scheduler.controller;

import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.options.ActorCreationOptions;
import org.ray.streaming.jobgraph.LanguageType;
import org.slf4j.Logger;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.rpc.call.RemoteCallWorker;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;

public class WorkerLifecycleController implements IWorkerLifecycleController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerLifecycleController.class);

  public WorkerLifecycleController() {
  }

  @Override
  public boolean createWorker(ExecutionVertex executionVertex, Map<String, Double> resources) {
    LOG.info("Start to create JobWorker actor for vertex: {} with resource: {}.",
        executionVertex.getVertexId(), resources);

    LanguageType language = executionVertex.getLanguageType();

    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(resources)
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTIONS)
        .createActorCreationOptions();
    
    RayActor actor = null;
    if (LanguageType.JAVA == language) {
      actor = RemoteCallWorker
          .createWorker(
              KryoUtils.writeToByteArray(
                  executionVertex.getJobConfig()), options);
    } else {
      // TODO
    }

    if (null == actor) {
      LOG.error("Create actor failed.");
      return false;
    }

    executionVertex.setWorkerActor(actor);
    executionVertex.getSlot().getActorCount().incrementAndGet();
    LOG.info("Create JobWorker actor succeeded, actor: {}, vertex: {}.",
        executionVertex.getWorkerActorId(), executionVertex.getVertexId());
    return true;
  }

  @Override
  public boolean destroyWorker(ExecutionVertex executionVertex) {
    if (null == executionVertex.getWorkerActor()) {
      LOG.error("Execution vertex does not have an actor!");
      return false;
    }

    RayObject<Boolean> destroyResult = RemoteCallWorker.destroyWorker(executionVertex.getWorkerActor());
    Ray.get(destroyResult.getId());

    if (!destroyResult.get()) {
      LOG.error("Failed to destroy JobWorker actor; {}.", executionVertex.getWorkerActorId());
      return false;
    }
    executionVertex.getSlot().getActorCount().decrementAndGet();
    LOG.info("Destroy JobWorker succeeded, actor: {}.", executionVertex.getWorkerActorId());
    return true;
  }

  @Override
  public boolean initWorker(RayActor rayActor, JobWorkerContext jobWorkerContext) {
    LOG.info("Start to init JobWorker [actor={}] with context: {}.",
        rayActor.getId(), jobWorkerContext);

    RayObject<Boolean> initResult = RemoteCallWorker.initWorker(rayActor, jobWorkerContext);
    Ray.get(initResult.getId());

    if (!initResult.get()) {
      LOG.error("Init JobWorker [actor={}] failed.", rayActor.getId());
      return false;
    }

    LOG.info("Init JobWorker [actor={}] succeed.", rayActor.getId());
    return true;
  }

  @Override
  public boolean startWorker(RayActor rayActor) {
    LOG.info("Start to start JobWorker [actor={}].", rayActor.getId());

    RayObject<Boolean> initResult = RemoteCallWorker.startWorker(rayActor);
    Ray.get(initResult.getId());

    if (!initResult.get()) {
      LOG.error("Start JobWorker [actor={}] failed.", rayActor.getId());
      return false;
    }

    LOG.info("Start JobWorker [actor={}] succeed.", rayActor.getId());
    return true;
  }
}