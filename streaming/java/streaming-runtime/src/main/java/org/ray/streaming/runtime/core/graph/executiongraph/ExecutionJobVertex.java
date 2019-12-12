package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
  private volatile List<ExecutionVertex> exeVertices = new ArrayList<>();
  private LanguageType languageType;
  private List<IntermediateResult> producedDataSets = new ArrayList<>();
  private List<IntermediateResult> inputs = new ArrayList<>();
  private ExecutionJobVertexState executionJobVertexState = ExecutionJobVertexState.NORMAL;
  private long buildTime;

  public ExecutionJobVertex(JobVertex jobVertex, ExecutionGraph executionGraph,
      Map<String, String> jobConf) {
    this.jobVertex = jobVertex;
    this.parallelism = jobVertex.getParallelism();
    this.languageType = jobVertex.getLanguageType();
    this.config = jobVertex.getConfig();
    this.maxParallelism = executionGraph.getMaxParallelism();
    this.jobConf = jobConf;
    this.exeVertices = createExecutionVertices(executionGraph);
   this.buildTime = executionGraph.getBuildTime();
  }

  private List<ExecutionVertex> createExecutionVertices(ExecutionGraph executionGraph) {
    List<ExecutionVertex> executionVertices = new ArrayList<>();
    for (int subTaskIndex = 0; subTaskIndex < jobVertex.getParallelism(); ++subTaskIndex) {
      ExecutionVertex executionVertex = new ExecutionVertex(
          jobVertex.getName(), executionGraph.incLastExecutionVertexIndex(), subTaskIndex, this)
          .withConfig(this.config);
      executionVertices.add(executionVertex);
    }
    return executionVertices;
  }

  public void attachExecutionVertex() {
    attachExecutionVertex(0);
  }

  public void attachExecutionVertex(int startIndex) {
    for (int index = startIndex; index < exeVertices.size(); ++index) {
      ExecutionVertex exeVertex = exeVertices.get(index);
      // skip vertex which has been marked as `TO_DEL`
      if (exeVertex.isToDelete()) {
        continue;
      }
      final int subIndex = index;

      // update input edges
      List<ExecutionEdge> inputEdges = new ArrayList<>();
      for (IntermediateResult result : inputs) {
        List<ExecutionEdge> list = new ArrayList<>();
        for (IntermediateResultPartition partition : result.getPartitions()) {
          ExecutionEdge executionEdge = partition.getConsumers().get(subIndex);
          if (!executionEdge.getSource().getProducer().isToDelete()) {
            list.add(executionEdge);
          }
        }
        inputEdges.addAll(list);
      }
      exeVertex.setInputEdges(inputEdges);
      // update output edges
      List<IntermediateResultPartition> resultPartitions = new ArrayList<>();
      for (IntermediateResult result : producedDataSets) {
        IntermediateResultPartition intermediateResultPartition = result.getPartitions()
            .get(subIndex);
        resultPartitions.add(intermediateResultPartition);
      }
      exeVertex.setResultPartitions(resultPartitions);
    }
  }

  public void connectNewIntermediateResultAsInput(JobEdge jobEdge, ExecutionJobVertex input) {
    // check partitioner
    IntermediateResult result = input.createAndAddIntermediateResult(jobEdge, this);
    inputs.add(result);
  }

  private IntermediateResult createAndAddIntermediateResult(JobEdge jobEdge,
      ExecutionJobVertex target) {
    IntermediateResult result = new IntermediateResult(this, target,
        jobEdge.getDistributionPattern());
    this.producedDataSets.add(result);
    return result;
  }

  public JobVertex getJobVertex() {
    return jobVertex;
  }

  public List<ExecutionVertex> getExeVertices() {
    return exeVertices;
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

  public LanguageType getLanguageType() {
    return languageType;
  }

  public List<IntermediateResult> getProducedDataSets() {
    return producedDataSets;
  }

  public List<ExecutionJobVertex> getOutputExecJobVertices() {
    List<ExecutionJobVertex> executionJobVertices = new ArrayList<>();
    for (IntermediateResult intermediateResult : producedDataSets) {
      executionJobVertices.add(intermediateResult.getConsumer());
    }
    return executionJobVertices;
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

  public void scaledown(int newParallelism) {
    int oldParallelism = parallelism;
    this.parallelism = newParallelism;
      // scale down
    this.markAsChanged();

    for (int subTaskIndex = newParallelism; subTaskIndex < oldParallelism; ++subTaskIndex) {
      // mark as TO_DEL state, should be removed from graph after scaling in is finished.
      exeVertices.get(subTaskIndex).setState(ExecutionVertexState.TO_DEL);
    }
    for (int subTaskIndex = 0; subTaskIndex < newParallelism; ++subTaskIndex) {
      exeVertices.get(subTaskIndex).setState(ExecutionVertexState.TO_UPDATE);
    }
    updateInputs();
    updateOutputs();
    reAttachInputExecutionVertices();
    reAttachOutputExecutionVertices();
  }

  public void scaledown(int newParallelism, Set<Integer> specificIndexes) {
    if (null == specificIndexes) {
      scaledown(newParallelism);
      return;
    }
    int oldParallelism = parallelism;
    this.parallelism = newParallelism;
    // scale down
    this.markAsChanged();
    ExecutionVertex[] newExeVertices = new ExecutionVertex[oldParallelism];
    int updateIndex = 0;
    int toDelIndex = newParallelism;
    // Rearrange subindex for specificindexes scaledown mode.
    for (int subTaskIndex = 0; subTaskIndex < oldParallelism; ++subTaskIndex) {
      ExecutionVertex executionVertex = exeVertices.get(subTaskIndex);
      if (specificIndexes.contains(exeVertices.get(subTaskIndex).getGlobalIndex())) {
        executionVertex.setState(ExecutionVertexState.TO_DEL);
        executionVertex.reAssignSubTaskIndex(toDelIndex);
        newExeVertices[toDelIndex] = executionVertex;
        toDelIndex++;
      } else {
        executionVertex.setState(ExecutionVertexState.TO_UPDATE);
        executionVertex.reAssignSubTaskIndex(updateIndex);
        newExeVertices[updateIndex] = executionVertex;
        updateIndex++;
      }
    }
    exeVertices = Arrays.stream(newExeVertices).collect(Collectors.toList());
    updateInputs();
    updateOutputs();
    reAttachInputExecutionVertices();
    reAttachOutputExecutionVertices();
  }


  public void scaleup(int newParallelism, ExecutionGraph executionGraph) {
    int oldParallelism = parallelism;
    this.parallelism = newParallelism;

    // scale up
    this.markAsChanged();

    for (int subTaskIndex = oldParallelism; subTaskIndex < newParallelism; ++subTaskIndex) {
      // to add
      ExecutionVertex executionVertex = new ExecutionVertex(jobVertex.getName(),
          executionGraph.incLastExecutionVertexIndex(), subTaskIndex, this).withConfig(this.config);
      exeVertices.add(executionVertex);
    }
    for (int subTaskIndex = 0; subTaskIndex < oldParallelism; ++subTaskIndex) {
      exeVertices.get(subTaskIndex).setState(ExecutionVertexState.TO_UPDATE);
    }
    updateInputs();
    updateOutputs();
    attachExecutionVertex(oldParallelism);
    reAttachInputExecutionVertices();
    reAttachOutputExecutionVertices();
  }

  private void updateInputs() {
    inputs.forEach(intermediateResult -> intermediateResult.updateConsumers(this));
  }

  private void updateOutputs() {
    producedDataSets.forEach(intermediateResult -> {
      intermediateResult.updateProducers(this);

      // If downstream execution job vertex is two input,
      // we need to update another input stream and its' parent input streams
      ExecutionJobVertex consumerVertex = intermediateResult.getConsumer();
      if (isTwoInput(consumerVertex)) {
        List<IntermediateResult> inputs = consumerVertex.inputs;
        for (IntermediateResult input : inputs) {
          ExecutionJobVertex producer = input.getProducer();
          // Is current execution job vertex, do nothing
          if (producer.getJobVertex().getId().equals(this.getJobVertex().getId())) {
            continue;
          }
          // Is another execution job vertex, mark execution vertices state as `TO_UPDATE`
          markExecutionVerticesAsToUpdate(producer);

          // Get another execution job vertex's parents and update their execution vertices
          List<IntermediateResult> parentInputs = producer.getProducedDataSets();

          if (parentInputs.size() == 0) {
            producer.markAsAffectedNeighbourParent();
          } else {
            producer.markAsAffectedNeighbour();

            for (IntermediateResult parentInput : parentInputs) {
              markExecutionVerticesAsToUpdate(parentInput.getProducer());
              parentInput.getProducer().markAsAffectedNeighbourParent();
            }
          }
        }
      }
    });
  }

  private void reAttachInputExecutionVertices() {
    inputs.forEach(intermediateResult -> intermediateResult.getProducer().attachExecutionVertex());
  }

  private void reAttachOutputExecutionVertices() {
    producedDataSets
        .forEach(intermediateResult -> intermediateResult.getConsumer().attachExecutionVertex());
  }

  public void markAsChanged() {
    executionJobVertexState = ExecutionJobVertexState.CHANGED;
  }

  public void markAsNormal() {
    executionJobVertexState = ExecutionJobVertexState.NORMAL;
  }

  public void markAsAffectedUpStream() {
    executionJobVertexState = ExecutionJobVertexState.AFFECTED_UP_STREAM;
  }

  public void markAsAffectedDownStream() {
    executionJobVertexState = ExecutionJobVertexState.AFFECTED_DOWN_STREAM;
  }

  public void markAsAffectedNeighbour() {
    executionJobVertexState = ExecutionJobVertexState.AFFECTED_NEIGHBOUR;
  }

  public void markAsAffectedNeighbourParent() {
    executionJobVertexState = ExecutionJobVertexState.AFFECTED_NEIGHBOUR_PARENT;
  }

  private boolean isTwoInput(ExecutionJobVertex exeJobVertex) {
    return 2 == exeJobVertex.inputs.size();
  }

  private void markExecutionVerticesAsToUpdate(ExecutionJobVertex exeJobVertex) {
    List<ExecutionVertex> exeVertices = exeJobVertex.getExeVertices();
    for (int subTaskIndex = 0; subTaskIndex < exeVertices.size(); ++subTaskIndex) {
      exeVertices.get(subTaskIndex).setState(ExecutionVertexState.TO_UPDATE);
    }
  }

  public ExecutionJobVertexState getExecutionJobVertexState() {
    return executionJobVertexState;
  }

  public boolean isChangedOrAffected() {
    return !executionJobVertexState.equals(ExecutionJobVertexState.NORMAL);
  }

  public boolean isSourceVertex() {
    return inputs.isEmpty();
  }

  public boolean isSinkVertex() {
    return producedDataSets.isEmpty();
  }

  public boolean isSourceAndSinkVertex() {
    return isSourceVertex() && isSinkVertex();
  }

  public List<ExecutionVertex> getNewbornVertices() {
    return exeVertices.stream()
        .filter(v -> v.getState() == ExecutionVertexState.TO_ADD)
        .collect(Collectors.toList());
  }

  public List<ExecutionVertex> getMoribundVertices() {
    return exeVertices.stream()
        .filter(v -> v.getState() == ExecutionVertexState.TO_DEL)
        .collect(Collectors.toList());
  }
}
