package org.ray.streaming.runtime.rpc;

import java.util.Map;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.options.ActorCreationOptions;
import org.ray.streaming.api.Language;
import org.ray.streaming.runtime.generated.RemoteCall;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ray call worker.
 * It takes the communication job from {@link JobMaster} to {@link JobWorker}.
 */
public class RemoteCallWorker extends RemoteCallBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  /**
   * Create JobWorker actor.
   *
   * @param language language type(java or python)
   * @param options actor creation options
   * @return JobWorker actor
   */
  public static RayActor<JobWorker> createWorker(Language language, ActorCreationOptions options) {
    if (Language.JAVA == language) {
      return Ray.createActor(JobWorker::new, options);
    } else {
      return Ray.createPyActor("ray.streaming.runtime.worker", "JobWorker", options);
    }
  }

  /**
   * Call JobWorker actor to init.
   *
   * @param actor target JobWorker actor
   * @param ctx JobWorker's context
   * @return init result
   */
  public static RayObject<Boolean> initWorker(RayActor<JobWorker> actor, JobWorkerContext ctx) {
    LOG.info("Call worker to init, actor: {}, context: {}.", actor.getId(), ctx);
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      //result = Ray.callPy((RayPyActor) actor, "init", );
    } else {
      // java
      result = actor.call(JobWorker::init, ctx);
    }

    LOG.info("Finish call worker to init.");
    return result;
  }

  /**
   * Call JobWorker actor to start.
   *
   * @param actor target JobWorker actor
   * @return start result
   */
  public static RayObject<Boolean> startWorker(RayActor<JobWorker> actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      result = Ray.callPy((RayPyActor) actor, "start");
    } else {
      // java
      result = actor.call(JobWorker::start);
    }

    LOG.info("Finish call worker to start.");
    return result;
  }

  /**
   * Call JobWorker actor to destroy.
   *
   * @param actor target JobWorker actor
   * @return destroy result
   */
  public static RayObject<Boolean> destroyWorker(RayActor<JobWorker> actor) {
    LOG.info("Call worker to destroy, actor is {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = actor.call(JobWorker::destroy);
    }

    LOG.info("Finish call worker to destroy.");
    return result;
  }

  private byte[] buildPythonWorkerContext(
      int taskId,
      RemoteCall.ExecutionGraph executionGraphPb,
      Map<String, String> jobConfig) {
    return RemoteCall.WorkerContext.newBuilder()
      .setTaskId(taskId)
      .putAllConf(jobConfig)
      .setGraph(executionGraphPb)
      .build()
      .toByteArray();
  }

}
