package org.ray.streaming.runtime.worker;

import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.annotation.RayRemote;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.config.global.TransferConfig;
import org.ray.streaming.runtime.config.types.TransferChannelType;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.processor.OneInputProcessor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.core.processor.StreamProcessor;
import org.ray.streaming.runtime.core.processor.TwoInputProcessor;
import org.ray.streaming.runtime.transfer.TransferHandler;
import org.ray.streaming.runtime.util.EnvUtil;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.ray.streaming.runtime.worker.task.OneInputStreamTask;
import org.ray.streaming.runtime.worker.task.SourceStreamTask;
import org.ray.streaming.runtime.worker.task.StreamTask;
import org.ray.streaming.runtime.worker.task.TwoInputStreamTask;

/**
 * The streaming worker implementation class, it is ray actor.
 */
@RayRemote
public class JobWorker implements IJobWorker {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  static {
    EnvUtil.loadNativeLibraries();
  }

  private StreamingWorkerConfig workerConfig;
  private JobWorkerContext workerContext;
  private ExecutionVertex executionVertex;

  private TransferHandler transferHandler;

  private StreamTask task;

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
    LOG.info("Init worker context {}. workerId: {}.", workerContext, workerContext.getWorkerId());
    this.workerContext = workerContext;
    this.executionVertex = KryoUtils.readFromByteArray(workerContext.getExecutionVertexBytes());
    this.workerConfig = new StreamingWorkerConfig(executionVertex.getJobConfig());

    //init transfer
    TransferChannelType channelType = workerConfig.transferConfig.channelType();
    if (TransferChannelType.NATIVE_CHANNEL == channelType) {
      transferHandler = new TransferHandler(
          getNativeCoreWorker(),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onWriterMessage", "([B)V"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onWriterMessageSync", "([B)[B"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onReaderMessage", "([B)V"),
          new JavaFunctionDescriptor(JobWorker.class.getName(), "onReaderMessageSync", "([B)[B"));
    }

    task = createStreamTask();

    return true;
  }

  private long getNativeCoreWorker() {
    long pointer = 0;
    if(Ray.internal() instanceof RayMultiWorkerNativeRuntime) {
      pointer = ((RayMultiWorkerNativeRuntime) Ray.internal()).getCurrentRuntime().getNativeCoreWorkerPointer();
    }
    return pointer;
  }

  private StreamTask createStreamTask() {
    StreamTask task;
    StreamProcessor streamProcessor = executionVertex.getStreamProcessor();
    if (streamProcessor instanceof SourceProcessor) {
      task = new SourceStreamTask(executionVertex.getVertexId(), streamProcessor, this);
    } else if (streamProcessor instanceof OneInputProcessor) {
      task = new OneInputStreamTask(executionVertex.getVertexId(), streamProcessor, this);
    } else if (streamProcessor instanceof TwoInputProcessor) {
      List<ExecutionEdge> inputEdges = this.executionVertex.getInputEdges();
      //TODO
      String leftStream = null;
      String rightStream = null;
      task = new TwoInputStreamTask(executionVertex.getVertexId(), streamProcessor, this, leftStream, rightStream);
    } else {
      throw new RuntimeException("Unsupported processor type:" + streamProcessor);
    }
    return task;
  }

  @Override
  public Boolean start() {
    try {
      task.start();
    } catch (Exception e) {
     LOG.error("Start worker [{}] occur error.", executionVertex.getVertexId(), e);
     return false;
    }
    return true;
  }

  @Override
  public void shutdown() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOG.info("Worker shutdown now.");
    }));
    System.exit(0);
  }

  @Override
  public Boolean destroy() {
    try {
      if (task != null) {
        task.close();
        task = null;
      }
    } catch (Exception e) {
      LOG.error("Close task has exception.", e);
      return false;
    }
    return true;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamingWorkerConfig getWorkerConfig() {
    return workerConfig;
  }
}
