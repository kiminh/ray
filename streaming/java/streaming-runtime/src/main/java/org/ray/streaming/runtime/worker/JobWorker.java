package org.ray.streaming.runtime.worker;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.base.Preconditions;
import org.ray.api.Ray;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.streaming.runtime.config.worker.TransferConfig;
import org.ray.streaming.runtime.core.transfer.TransferHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.JobEdge;
import org.ray.streaming.runtime.core.processor.OneInputProcessor;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.core.processor.TwoInputProcessor;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.TestHelper;
import org.ray.streaming.runtime.worker.task.ControlMessage;
import org.ray.streaming.runtime.worker.task.SourceStreamTask;
import org.ray.streaming.runtime.worker.task.StreamTask;

/**
 * The streaming worker implementation class, it is ray actor.
 */
@RayRemote
public class JobWorker implements IJobWorker {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  /**
   * Worker(execution vertex) configuration
   */
  private StreamingWorkerConfig workerConfig;

  /**
   * The context of job worker
   */
  protected JobWorkerContext workerContext;

  private ExecutionVertex executionVertex;
  private byte[] executionVertexBytes;

  /**
   * The thread of stream task
   */
  private StreamTask task;

  /**
   * Control message
   */
  private volatile boolean hasMessage = false;

  private TransferHandler transferHandler;

  private Object lock = new Object();

  public JobWorker() {
  }

  public JobWorker(final byte[] confBytes) {
    LOG.info("Job worker begin init.");

    Map<String, String> confMap = KryoUtils.readFromByteArray(confBytes);
    workerConfig = new StreamingWorkerConfig(confMap);
    LOG.info("Job worker conf is {}.", workerConfig.configMap);

    LOG.info("Job worker init success.");
  }

  @Override
  public Boolean init(JobWorkerContext workerContext) {
    LOG.info("Init worker context {}. workerId: {}.", workerContext, workerContext.workerId);
    ExecutionVertex executionVertex = null;
    if (null != workerContext.executionVertexBytes) {
      executionVertex = KryoUtils.readFromByteArray(workerContext.executionVertexBytes);
    }
    this.workerContext = workerContext;
    this.workerConfig = new StreamingWorkerConfig(workerContext.conf);
    this.executionVertex = executionVertex;

    //init transfer
    String channelType = workerConfig.transferConfig.chennelType();
    if (channelType.equalsIgnoreCase(TransferConfig.NATIVE_CHANNEL)) {
      transferHandler = new TransferHandler(
          getNativeCoreWorker(),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onWriterMessage", "([B)V"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onWriterMessageSync", "([B)[B"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onReaderMessage", "([B)V"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onReaderMessageSync", "([B)[B"));
    }

    task


    return true;
  }

  private long getNativeCoreWorker() {
    long pointer = 0;
    if(Ray.internal() instanceof RayMultiWorkerNativeRuntime) {
      pointer = ((RayMultiWorkerNativeRuntime) Ray.internal()).getCurrentRuntime().getNativeCoreWorkerPointer();
    }
    return pointer;
  }

  @Override
  public Boolean start() {
    
  }

  // ----------------------------------------------------------------------
  // Job Worker Shutdown and Destroy
  // ----------------------------------------------------------------------

  @Override
  public void shutdown() {
    LOG.info("Worker {} shutdown.", workerContext.workerId);

    try {
      if (task != null) {
        task.closeWithoutExit();
        task = null;
      }
    } catch (Exception e) {
      LOG.error("Close runner has exception.", e);
    }

    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        LOG.info("Worker {} shutdown now.", workerContext.workerId);
      }
    });
    System.exit(0);
  }

  @Override
  public Boolean destroy() {
    LOG.info("Worker {} shutdown without reconstruction.", workerContext.workerId);

    try {
      if (task != null) {
        task.closeWithoutExit();
        task = null;
      }
    } catch (Exception e) {
      LOG.error("Close runner has exception.", e);
    }

    // Single process UT pattern does not exit, just stop the task thread
    if (TestHelper.isUTPattern()) {
      return true;
    }

    LOG.info("Ray.exitActor.");
    // Exit this actor process.
    Ray.exitActor();
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
        LOG.info("Worker {} before inserting, mailbox: {}, hasMessage: {}.", workerContext.workerId,
            workerContext.mailbox, hasMessage);

        workerContext.mailbox.put(message);
        hasMessage = true;

        LOG.info("Worker {} after inserting, mailbox: {}, hasMessage: {}.", workerContext.workerId,
            workerContext.mailbox, hasMessage);
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
      LOG.info("Worker {} before polling, mailbox: {}, hasMessage: {}.", workerContext.workerId,
          workerContext.mailbox, hasMessage);

      message = workerContext.mailbox.poll();
      hasMessage = !workerContext.mailbox.isEmpty();

      LOG.info("Worker {} polled message from mailbox: {}, remaining: {}, hasMessage: {}.",
          workerContext.workerId, message, workerContext.mailbox, hasMessage);
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

  public void setContext(JobWorkerContext context) {
    this.workerContext = context;
  }

  public JobWorkerContext getContext() {
    return workerContext;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamTask getTask() {
    return task;
  }

  private StreamTask createStreamTask() {
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


  private static StreamingWorkerConfig getJobWorkerConf(final byte[] confBytes) {
    Map<String, String> confMap = KryoUtils.readFromByteArray(confBytes);
    return new StreamingWorkerConfig(confMap);
  }

  private Map<String, String> getJobWorkerTags() {
    Map<String, String> workerTags = new HashMap<>();
    workerTags.put("worker_name", this.workerContext.workerName);
    workerTags.put("op_name", this.workerContext.opName);
    workerTags.put("worker_id", this.workerContext.workerId.toString());
    return workerTags;
  }

  public StreamingWorkerConfig getWorkerConfig() {
    return this.workerConfig;
  }
}
