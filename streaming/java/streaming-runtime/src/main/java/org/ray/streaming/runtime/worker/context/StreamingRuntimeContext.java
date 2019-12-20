package org.ray.streaming.runtime.worker.context;

import java.util.Map;

import org.ray.streaming.api.context.RuntimeContext;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;

/**
 * Use Ray to implement RuntimeContext.
 */
public class StreamingRuntimeContext implements RuntimeContext {
  private int globalTaskId;
  private int subTaskIndex;
  private int parallelism;
  private long batchId;
  private Map<String, String> jobConfig;

  public StreamingRuntimeContext(ExecutionVertex executionVertex, Map<String, String> jobConfig, int parallelism) {
    this.globalTaskId = executionVertex.getVertexId();
    this.subTaskIndex = executionVertex.getVertexIndex();
    this.parallelism = parallelism;
    this.jobConfig = jobConfig;
  }

  @Override
  public int getTaskId() {
    return globalTaskId;
  }

  @Override
  public int getTaskIndex() {
    return subTaskIndex;
  }

  @Override
  public int getParallelism() {
    return parallelism;
  }

  @Override
  public Long getBatchId() {
    return batchId;
  }

  public void setBatchId(long batchId) {
    this.batchId = batchId;
  }
}
