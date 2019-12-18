package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.ray.streaming.runtime.config.types.OperatorType;
import org.ray.streaming.runtime.core.graph.jobgraph.JobEdge;
import org.ray.streaming.runtime.core.graph.jobgraph.JobVertex;
import org.ray.streaming.runtime.core.graph.jobgraph.LanguageType;

public class ExecutionJobVertex implements Serializable {

  private final Map<String, Object> config;
  private final Map<String, String> jobConf;
  // mutable
  private int parallelism;
  private int maxParallelism;
  private JobVertex jobVertex;
  private LanguageType languageType;
  private OperatorType operatorType;
  private volatile List<ExecutionVertex> executionVertices;

  private List<IntermediateResult> outputDataSet = new ArrayList<>();
  private List<IntermediateResult> inputDataSet = new ArrayList<>();
  private long buildTime;

  public ExecutionJobVertex(JobVertex jobVertex, ExecutionGraph executionGraph,
      Map<String, String> jobConf) {
    this.jobVertex = jobVertex;
    this.parallelism = jobVertex.getParallelism();
    this.maxParallelism = executionGraph.getMaxParallelism();
    this.languageType = jobVertex.getLanguageType();
    this.config = jobVertex.getConfig();
    this.jobConf = jobConf;
    this.executionVertices = createExecutionVertices(executionGraph);
    this.buildTime = executionGraph.getBuildTime();
  }

  private List<ExecutionVertex> createExecutionVertices(ExecutionGraph executionGraph) {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    for (int subTaskIndex = 0; subTaskIndex < parallelism; ++subTaskIndex) {
      ExecutionVertex executionVertex = new ExecutionVertex(
          jobVertex.getName(), executionGraph.incLastExecutionVertexIndex(), subTaskIndex, this)
          .withConfig(this.config);
      executionVertices.add(executionVertex);
    }
    return executionVertices;\
  }

  public void attachExecutionVertex() {
    attachExecutionVertex(0);
  }

  public void attachExecutionVertex(int startIndex) {
    for (int index = startIndex; index < executionVertices.size(); ++index) {
      ExecutionVertex exeVertex = executionVertices.get(index);
      final int subIndex = index;

      // update input edges
      List<ExecutionEdge> inputEdges = new ArrayList<>();
      for (IntermediateResult result : inputDataSet) {
        List<ExecutionEdge> list = new ArrayList<>();
        for (IntermediateResultPartition partition : result.getPartitions()) {
          ExecutionEdge executionEdge = partition.getConsumers().get(subIndex);
          list.add(executionEdge);
        }
        inputEdges.addAll(list);
      }
      exeVertex.setInputEdges(inputEdges);
      // update output edges
      List<IntermediateResultPartition> resultPartitions = new ArrayList<>();
      for (IntermediateResult result : outputDataSet) {
        IntermediateResultPartition intermediateResultPartition = result.getPartitions()
            .get(subIndex);
        resultPartitions.add(intermediateResultPartition);
      }
      exeVertex.setOutputPartitions(resultPartitions);
    }
  }

  public void connectNewIntermediateResultAsInput(JobEdge jobEdge, ExecutionJobVertex input) {
    // check partitioner
    IntermediateResult result = input.createAndAddIntermediateResult(jobEdge, this);
    inputDataSet.add(result);
  }

  private IntermediateResult createAndAddIntermediateResult(JobEdge jobEdge,
      ExecutionJobVertex target) {
    IntermediateResult result = new IntermediateResult(this, target,
        jobEdge.getDistributionPattern());
    this.outputDataSet.add(result);
    return result;
  }

  public JobVertex getJobVertex() {
    return jobVertex;
  }

  public List<ExecutionVertex> getExecutionVertices() {
    return executionVertices;
  }

  public int getParallelism() {
    return parallelism;
  }

  public int getMaxParallelism() {
    return maxParallelism;
  }

  public void setMaxParallelism(int maxParallelism) {
    this.maxParallelism = maxParallelism;
  }


  public Map<String, Object> getConfig() {
    return config;
  }

  public Map<String, String> getJobConf() {
    return jobConf;
  }

  public long getBuildTime() {
    return buildTime;
  }


  public boolean isSourceVertex() {
    return inputDataSet.isEmpty();
  }

  public boolean isSinkVertex() {
    return outputDataSet.isEmpty();
  }

  public boolean isSourceAndSinkVertex() {
    return isSourceVertex() && isSinkVertex();
  }
}
