package org.ray.streaming.runtime.worker;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Preconditions;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.ray.api.Checkpointable;
import org.ray.api.Ray;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.api.id.UniqueId;
import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.utils.KryoUtils;
import org.ray.streaming.runtime.utils.Serializer;
import org.ray.streaming.runtime.worker.task.ControlMessage;
import org.ray.streaming.runtime.worker.task.StreamTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The streaming worker implementation class, it is ray actor.
 */
@RayRemote
public class JobWorker implements IJobWorker, Checkpointable {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);


  /**
   * The context of job worker
   */
  protected JobWorkerContext context;

  /**
   * The thread of stream task
   */
  private StreamTask task;

  /**
   * Rollback relations fields
   */
  public Collection<String> abnormalQueues;
  public AtomicBoolean wasReconstructed = new AtomicBoolean(false);
  private int rollbackCnt = 0;

  /**
   * The flag of check if need checkpoint
   */
  private boolean shouldCheckpoint = false;

  /**
   * Worker(execution vertex) configuration
   */
  private StreamingWorkerConfig conf;
  private ExecutionVertex executionVertex;
  private byte[] executionVertexBytes;

  /**
   * Control message
   */
  private volatile boolean hasMessage = false;
  private Object lock = new Object();

  public JobWorker() {
  }

  public JobWorker(final byte[] confBytes) {
    LOG.info("Job worker begin init.");

    Map<String, String> confMap = KryoUtils.readFromByteArray(confBytes);
    conf = new StreamingWorkerConfig(confMap);
    LOG.info("Job worker conf is {}.", conf.configMap);

    // init state backend
    //this.stateBackend = StateBackendFactory.getStateBackend(conf);

    LOG.info("Job worker init success.");
  }

  // ----------------------------------------------------------------------
  // Ray Checkpointable Interface
  // ----------------------------------------------------------------------

  /**
   * Whether this actor needs to be checkpointed.
   */
  @Override
  public boolean shouldCheckpoint(CheckpointContext checkpointContext) {
    return false;
  }

  /**
   * Ray Checkpointable Interface Save a checkpoint to persistent storage.
   */
  @Override
  public void saveCheckpoint(ActorId actorId, UniqueId checkpointId) {

  }

  /**
   * Ray Checkpointable Interface Load actor's previous checkpoint, and restore actor's state.
   */
  @Override
  public UniqueId loadCheckpoint(ActorId actorId, List<Checkpoint> availableCheckpoints) {
    return null;
  }

  @Override
  public void checkpointExpired(ActorId actorId, UniqueId checkpointId) {
  }

  // ----------------------------------------------------------------------
  // Job Worker Start
  // ----------------------------------------------------------------------
  @Override
  public void registerContext(JobWorkerContext ctx) {
    LOG.info("Register worker context {}. workerId: {}.", ctx, ctx.workerId);
    boolean isFirstRegister = (this.context == null);
    ExecutionVertex executionVertex = null;
    if (null != ctx.executionVertexBytes) {
      executionVertex = KryoUtils.readFromByteArray(ctx.executionVertexBytes);
    }

    this.context = ctx;
    this.conf = new StreamingWorkerConfig(ctx.conf);

    this.hasMessage = !context.mailbox.isEmpty();
    this.executionVertex = executionVertex;

    Configuration configuration = new BaseConfiguration();
    configuration.addProperty(WorkerConfig.STATE_VERSION, conf.workerConfig.stateVersion());
    RuntimeContext.setRuntimeEnv(new RuntimeEnvironment(configuration));

    // init extra resource
    String extraResource = conf.extraResourceConfig.extraResourceUrl();
    if (!StringUtils.isEmpty(extraResource)) {
      if (initExtraResource(extraResource)) {
        LOG.info("Initiate extra resource success. Extra resource is {}.", extraResource);
      } else {
        LOG.info("Initiate extra resource fail. Extra resource is {}.", extraResource);
      }
    } else {
      LOG.info("No extra resource need for job. Skip initiating extra resource.");
    }
  }

  // ----------------------------------------------------------------------
  // Job Worker Destroy
  // ----------------------------------------------------------------------

  @Override
  public boolean destroy() {
    shouldCheckpoint = true;

    try {
      if (task != null) {
        // make sure the runner is closed
        task.close();
        task = null;
      }
    } catch (Exception e) {
      LOG.error("Close runner has exception.", e);
    }

    return true;
  }

  // ----------------------------------------------------------------------
  // Job Worker Auto Scale
  // ----------------------------------------------------------------------

  /**
   * Inserts control message at the tail of this queue, waiting for space to become available if the
   * queue is full.
   *
   * @return true if put successfully
   */
  private boolean insertControlMessage(ControlMessage message) {
    try {
      synchronized (lock) {
        LOG.info("Worker {} before inserting, mailbox: {}, hasMessage: {}.", context.workerId,
            context.mailbox, hasMessage);

        context.mailbox.put(message);
        hasMessage = true;

        LOG.info("Worker {} after inserting, mailbox: {}, hasMessage: {}.", context.workerId,
            context.mailbox, hasMessage);
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to insert control message to mailbox.", e);
      return false;
    }
    return true;
  }

  /**
   * Retrieves and removes control message at the head of this queue, or returns {@code null} if
   * this queue is empty.
   *
   * @return control message at head of this queue, or {@code null} if this queue is empty.
   */
  public ControlMessage pollControlMessage() {
    ControlMessage message;
    synchronized (lock) {
      LOG.info("Worker {} before polling, mailbox: {}, hasMessage: {}.", context.workerId,
          context.mailbox, hasMessage);

      message = context.mailbox.poll();
      hasMessage = !context.mailbox.isEmpty();

      LOG.info("Worker {} polled message from mailbox: {}, remaining: {}, hasMessage: {}.",
          context.workerId, message, context.mailbox, hasMessage);
      return message;
    }
  }

  /**
   * Check whether mailbox has control message or not (lock free)
   *
   * @return true if worker mailbox still has message.
   */
  public boolean hasControlMessage() {
    return this.hasMessage;
  }

  /**
   * Hot update worker context
   *
   * @param newContext job worker context which is updated
   */
  @Override
  public void updateContext(JobWorkerContext newContext) {
    try {
      LOG.info("Insert update context control message into mailbox.");
      ControlMessage<JobWorkerContext> message = new ControlMessage(newContext, UPDATE_CONTEXT);
      insertControlMessage(message);
    } catch (Exception e) {
      LOG.error("Failed to update context.", e);
    }
  }


  @Override
  public void shutdown() {
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Worker shutdown now.");
      }
    });
    System.exit(0);
  }



  private File downloadFileByHttpClient(String url, File localTargetFile) throws IOException {
    HttpClientUtil.getFile(url, localTargetFile);
    return localTargetFile;
  }

  private File downloadFileByOssClient(String endpoint, String accessKeyId, String accessKeySecret,
      String bucket, String objectKey, File localTargetFile) {
    OssClientUtil oss = new OssClientUtil(endpoint, accessKeyId, accessKeySecret);
    oss.getFile(bucket, objectKey, localTargetFile);
    return localTargetFile;
  }

  public WorkerMetrics getMetrics() {
    return metrics;
  }

  public void setContext(JobWorkerContext context) {
    this.context = context;
  }

  public JobWorkerContext getContext() {
    return context;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamTask getTask() {
    return task;
  }

  private StreamTask createStreamTask(long checkpointId) {
    StreamTask task;
    Processor processor = this.executionVertex.getExeJobVertex().getJobVertex().getProcessor();
    if (processor instanceof SourceProcessor) {
      // source actor
      LOG.info("Create source stream task with {}, operator is {}.",
          checkpointId, conf.workerConfig.operatorName());
      task = new SourceStreamTask(processor, checkpointId,
          stateBackend, this);
    } else if (processor instanceof OneInputProcessor) {
      LOG.info("Create one input stream task with {}, operator is {}.",
          checkpointId, conf.workerConfig.operatorName());
      task = new OneInputStreamTask(processor, checkpointId, stateBackend, this);
    } else if (processor instanceof TwoInputProcessor) {
      LOG.info("Create two input stream task with {}, operator is {}.",
          checkpointId, conf.workerConfig.operatorName());
      List<JobEdge> jobEdges = this.executionVertex.getExeJobVertex().getJobVertex().getInputs();
      Preconditions.checkState(jobEdges.size() == 2,
          "Two input vertex input edge size must be 2.");
      String leftStream = jobEdges.get(0).getSource().getProducer().getId().toString();
      String rightStream = jobEdges.get(1).getSource().getProducer().getId().toString();
      task = new TwoInputStreamTask(processor, checkpointId,
          stateBackend, this,
          leftStream,
          rightStream);
    } else {
      throw new RuntimeException("Unsupported processor type: " + processor);
    }
    return task;
  }

  private String getJobWorkerContextKey() {
    return conf.checkpointConfig.jobWorkerContextCpPrefixKey()
        + conf.commonConfig.jobName()
        + "_" + conf.workerConfig.workerId();
  }

  private static StreamingWorkerConfig getJobWorkerConf(final byte[] confBytes) {
    Map<String, String> confMap = KryoUtils.readFromByteArray(confBytes);
    return new StreamingWorkerConfig(confMap);
  }

  private Map<String, String> getJobWorkerTags() {
    Map<String, String> workerTags = new HashMap<>();
    workerTags.put("worker_name", this.context.workerName);
    workerTags.put("op_name", this.context.opName);
    workerTags.put("worker_id", this.context.workerId.toString());
    return workerTags;
  }


  public class WorkerState {

    JobWorker.StateType type;

    public WorkerState() {
      this.type = JobWorker.StateType.INIT;
    }

    public void setType(JobWorker.StateType type) {
      this.type = type;
    }

    public JobWorker.StateType getType() {
      return type;
    }
  }

  public enum StateType {
    /**
     * INIT/RUNNING/WAIT_ROLLBACK
     */
    INIT(1),
    RUNNING(2),
    WAIT_ROLLBACK(3);

    private int value;

    StateType(int value) {
      this.value = value;
    }

    public int getValue() {
      return value;
    }
  }

  public StreamingWorkerConfig getConf() {
    return this.conf;
  }
}