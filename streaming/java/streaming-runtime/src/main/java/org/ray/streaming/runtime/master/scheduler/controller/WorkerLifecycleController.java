package org.ray.streaming.runtime.master.scheduler.controller;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.options.ActorCreationOptions;
import org.slf4j.Logger;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.LanguageType;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.rpc.call.RemoteCallWorker;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.worker.JobWorkerContext;

public class WorkerLifecycleController implements IWorkerLifecycleController {

  private static final Logger LOG = LoggerFactory.getLogger(WorkerLifecycleController.class);

  private final ResourceManager resourceManager;

  public WorkerLifecycleController(ResourceManager resourceManager) {
    this.resourceManager = resourceManager;
  }

  @Override
  public boolean createWorker(ExecutionVertex executionVertex) {
    LOG.info("Start to create JobWorker actor for vertex: {}.", executionVertex.getId());
    
    Map<String, Double> resources = resourceManager.allocateActor(executionVertex);
    LanguageType language = executionVertex.getExeJobVertex().getLanguageType();

    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(resources)
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTIONS)
        .createActorCreationOptions();
    
    RayActor actor = null;
    if (LanguageType.JAVA == language) {
      actor = RemoteCallWorker
          .createWorker(
              KryoUtils.writeToByteArray(
                  executionVertex.getExecutionConfig().getConfiguration().toStringMap()), options);
    } else {
      // TODO
    }

    if (null == actor) {
      LOG.error("Create actor failed.");
      return false;
    }

    executionVertex.setActor(actor);
    executionVertex.getSlot().getActorCount().incrementAndGet();
    LOG.info("Create JobWorker actor succeeded, actor: {}, vertex: {}.",
        executionVertex.getActorId(), executionVertex.getId());
    return true;
  }

  @Override
  public boolean destroyWorker(ExecutionVertex executionVertex) {
    if (null == executionVertex.getActor()) {
      LOG.error("Execution vertex does not have an actor!");
      return false;
    }

    LOG.info("Start to destroy JobWorker actor for vertex: {}.", executionVertex.getActorId());
    resourceManager.deallocateActor(executionVertex);

    RayObject<Boolean> destroyResult = RemoteCallWorker.destroyWorker(executionVertex.getActor());
    Ray.get(destroyResult.getId());

    if (!destroyResult.get()) {
      LOG.error("Failed to destroy JobWorker actor; {}.", executionVertex.getActorId());
      return false;
    }
    executionVertex.getSlot().getActorCount().decrementAndGet();
    LOG.info("Destroy JobWorker succeeded, actor: {}.", executionVertex.getActorId());
    return true;
  }

  @Override
  public boolean createWorkers(List<ExecutionVertex> executionVertices) {
    return asyncExecute(executionVertices, true);
  }

  @Override
  public boolean destroyWorkers(List<ExecutionVertex> executionVertices) {
    return asyncExecute(executionVertices, false);
  }

  private boolean asyncExecute(
      List<ExecutionVertex> executionVertices,
      boolean isToCreate) {

//    final Object asyncContext = Ray.getAsyncContext();

    List<CompletableFuture<Boolean>> futureResults = executionVertices.stream().map(vertex ->
        CompletableFuture.supplyAsync(() -> {
//          Ray.setAsyncContext(asyncContext);
          return isToCreate ? createWorker(vertex) : destroyWorker(vertex);
        })).collect(Collectors.toList());

    List<Boolean> createSucceeded = futureResults.stream().map(CompletableFuture::join)
        .collect(Collectors.toList());

    if (createSucceeded.stream().anyMatch(x -> !x)) {
      LOG.error("Not all futures return true, check ResourceManager'log the detail.");
      return false;
    }
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

    LOG.error("Init JobWorker [actor={}] succeed.", rayActor.getId());
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

    LOG.error("Start JobWorker [actor={}] succeed.", rayActor.getId());
    return true;
  }
}