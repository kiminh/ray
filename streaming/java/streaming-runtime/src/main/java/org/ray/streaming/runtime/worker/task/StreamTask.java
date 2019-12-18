package org.ray.streaming.runtime.worker.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.id.ActorId;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.config.worker.WorkerConfig;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.graph.executiongraph.IntermediateResultPartition;
import org.ray.streaming.runtime.core.graph.jobgraph.IntermediateDataSet;
import org.ray.streaming.runtime.core.graph.jobgraph.JobVertex;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.transfer.DataReader;
import org.ray.streaming.runtime.core.transfer.DataWriter;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.context.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Task execution abstract class.
 */
public abstract class StreamTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected int taskId;
  protected Processor processor;
  protected JobWorker jobWorker;
  protected DataReader reader;

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

    List<Collector> collectors = new ArrayList<>();

    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();
    JobVertex jobVertex = executionVertex.getExecutionJobVertex().getJobVertex();
    List<IntermediateDataSet> result = jobVertex.getResults();
    if (!result.isEmpty()) {
      for (int index = 0; index < result.size(); index++) {

        IntermediateResultPartition resultPartition = executionVertex.getOutputPartitions().get(index);
        List<String> outputChannelIDs = new ArrayList<>();
        List<ActorId> targetActorIds = new ArrayList<>();
        resultPartition.getConsumers().forEach(executionEdge -> {
          executionEdge.getTarget().getOutputQueues().forEach((actorId, queueName) -> {
            outputChannelIDs.add(queueName);
            targetActorIds.add(actorId);
          });
        });

        DataWriter writer = new DataWriter(outputChannelIDs, targetActorIds, queueConf);
        LOG.info("Create DataWriter succeed.");
        collectors.add(jobVertex.getOutputs().get(index));
      }
    }

    //consumer
    List<String> inputChannelIDs = new ArrayList<>();
    List<ActorId> fromActorIds = new ArrayList<>();
    executionVertex.getInputQueues().forEach((actorId, queueName) -> {
      inputChannelIDs.add(queueName);
      fromActorIds.add(actorId);
    });
    reader = new DataReader(inputChannelIDs, fromActorIds, queueConf);


    Map<String, String> conf = new HashMap<>(executionVertex.getExecutionJobVertex().getJobConf());
    RuntimeContext runtimeContext = new StreamingRuntimeContext(executionVertex, conf);
    processor.open(collectors, runtimeContext);
  }

  public void start() {
    this.thread.start();
    LOG.info("started {}-{}", this.getClass().getSimpleName(), taskId);
  }
}
