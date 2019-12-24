package org.ray.streaming.runtime.worker.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.config.worker.WorkerConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.transfer.ChannelID;
import org.ray.streaming.runtime.core.transfer.DataReader;
import org.ray.streaming.runtime.core.transfer.DataWriter;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.core.transfer.collector.StreamCollector;
import org.ray.streaming.runtime.worker.context.StreamingRuntimeContext;

/**
 * Task execution abstract class.
 */
public abstract class StreamTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected int taskId;
  protected Processor processor;
  protected JobWorker jobWorker;
  protected DataReader reader;

  List<Collector> collectors = new ArrayList<>();

  protected volatile boolean running = true;

  //Execution thread
  private Thread thread;

  protected StreamTask(int taskId, Processor processor, JobWorker jobWorker) {
    this.taskId = taskId;
    this.processor = processor;
    this.jobWorker = jobWorker;

    prepareTask();

    this.thread = new Thread(Ray.wrapRunnable(this), this.getClass().getName() + "-" + System.currentTimeMillis());
  }

  private void prepareTask() {
    Map<String, String> queueConf = new HashMap<>();
    queueConf.putAll(jobWorker.getWorkerConfig().workerConfig2Map());
    queueConf.put(WorkerConfig.taskId, Ray.getRuntimeContext().getCurrentJobId().toString());

    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();
    ExecutionJobVertex executionJobVertex = executionVertex.getExecutionJobVertex();

    // consumer
    List<ExecutionEdge> outputEdges = executionVertex.getOutputEdges();
    Map<String, ActorId> outputActor = new HashMap<>();
    for (ExecutionEdge edge : outputEdges) {
      String queueName = ChannelID.genIdStr(
          taskId, edge.getConsumer().getVertexId(), executionVertex.getBuildTime());
      outputActor.put(queueName, edge.getConsumer().getWorkerActorId());
    }
    if (!outputActor.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      List<ActorId> targetActorIds = new ArrayList<>();
      outputActor.forEach((vertexId, actorId) -> {
        channelIDs.add(vertexId);
        targetActorIds.add(actorId);
      });
      DataWriter writer = new DataWriter(channelIDs, targetActorIds, queueConf);
      collectors.add(new StreamCollector(channelIDs, writer,
          executionJobVertex.getOutputEdges().get(0).getPartition()));
    }

    // producer
    List<ExecutionEdge> inputEdges = executionVertex.getInputEdges();
    Map<String, ActorId> inputActorIds = new HashMap<>();
    for (ExecutionEdge edge : inputEdges) {
      String queueName = ChannelID.genIdStr(
          edge.getProducer().getVertexId(), taskId, executionVertex.getBuildTime());
      inputActorIds.put(queueName, edge.getProducer().getWorkerActorId());
    }
    if (!inputActorIds.isEmpty()) {
      List<String> channelIDs = new ArrayList<>();
      List<ActorId> fromActorIds = new ArrayList<>();
      inputActorIds.forEach((k, v) -> {
        channelIDs.add(k);
        fromActorIds.add(v);
      });
      LOG.info("Register queue consumer, queues {}.", channelIDs);
      reader = new DataReader(channelIDs, fromActorIds, queueConf);
    }

    RuntimeContext runtimeContext = new StreamingRuntimeContext(executionVertex,
        jobWorker.getWorkerConfig().configMap, executionVertex.getParallelism());
    processor.open(collectors, runtimeContext);
  }

  public void start() {
    this.thread.start();
    LOG.info("started {}-{}", this.getClass().getSimpleName(), taskId);
  }

  public void close() {
    this.running = false;
    if (thread.isAlive() && !Ray.getRuntimeContext().isSingleProcess()) {
      Runtime.getRuntime().halt(0);
      System.exit(0);
      LOG.warn("runtime halt 0");
    }
    LOG.info("Stream task close success.");
  }
}
