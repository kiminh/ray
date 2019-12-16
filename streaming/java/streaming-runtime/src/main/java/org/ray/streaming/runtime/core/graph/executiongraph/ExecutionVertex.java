package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.ray.streaming.runtime.config.Configuration;
import org.ray.streaming.runtime.config.types.OperatorType;
import org.ray.streaming.runtime.core.graph.jobgraph.JobVertex.OpInfo;
import org.ray.streaming.runtime.core.resource.Slot;
import org.ray.streaming.runtime.worker.JobWorker;

public class ExecutionVertex implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionVertex.class);

  private final AbstractID id = new AbstractID();
  private final int globalIndex;
  private int subTaskIndex;
  private String taskNameWithSubtask;
  private final ExecutionJobVertex exeJobVertex;

  // Those fields are mutable.
  private ExecutionVertexState state = ExecutionVertexState.TO_ADD;
  private ExecutionConfig executionConfig;
  private List<IntermediateResultPartition> resultPartitions;
  private List<ExecutionEdge> inputEdges;

  private Map<ActorId, String> inputQueues = new HashMap<>();
  private Map<String, RayActor> inputActors = new HashMap<>();
  private Map<ActorId, String> outputQueues = new HashMap<>();
  private Map<String, RayActor> outputActors = new HashMap<>();

  private OperatorType roleInChangedSubDag = OperatorType.TRANSFORM;

  // Those fields will be set or updated after actor allocation finished.
  private RayActor<JobWorker> workerActor;
  private Slot slot;

  public ExecutionVertex(String taskName, int globalIndex, int subTaskIndex,
      ExecutionJobVertex exeJobVertex) {
    this.subTaskIndex = subTaskIndex;
    this.taskNameWithSubtask = taskName + "-" + subTaskIndex;
    this.exeJobVertex = exeJobVertex;
    this.globalIndex = globalIndex;
  }

  public void setSlotIfNotExist(Slot slot) {
    if (null == this.slot) {
      this.slot = slot;
    }
  }

  public void setState(ExecutionVertexState state) {
    this.state = state;
  }

  public void reAssignSubTaskIndex(int subTaskIndex) {
    this.subTaskIndex = subTaskIndex;
    String[] taskNameList =  this.taskNameWithSubtask.split("-");
    taskNameList[taskNameList.length - 1] = String.valueOf(subTaskIndex);
    this.taskNameWithSubtask = String.join("-", taskNameList);
  }

  public void setActor(RayActor actor) {
    this.workerActor = actor;
  }

  public void setResultPartitions(List<IntermediateResultPartition> resultPartitions) {
    this.resultPartitions = resultPartitions;
  }

  public void setInputEdges(List<ExecutionEdge> inputEdges) {
    this.inputEdges = inputEdges;
  }

  public String getActorName() {
    return String.valueOf(globalIndex);
  }

  public AbstractID getId() {
    return id;
  }

  public int getSubTaskIndex() {
    return subTaskIndex;
  }

  public int getGlobalIndex() {
    return globalIndex;
  }

  public String getTaskNameWithSubtask() {
    return taskNameWithSubtask;
  }

  public ExecutionJobVertex getExeJobVertex() {
    return exeJobVertex;
  }

  public Slot getSlot() {
    return slot;
  }

  public ExecutionVertexState getState() {
    return state;
  }

  public RayActor getActor() {
    return workerActor;
  }

  public ExecutionConfig getExecutionConfig() {
    return executionConfig;
  }

  public List<IntermediateResultPartition> getResultPartitions() {
    return resultPartitions;
  }

  public List<ExecutionEdge> getInputEdges() {
    return inputEdges;
  }

  public Map<ActorId, String> getInputQueues() {
    return inputQueues;
  }

  public Map<String, RayActor> getInputActors() {
    return inputActors;
  }

  public Map<ActorId, String> getOutputQueues() {
    return outputQueues;
  }

  public Map<String, RayActor> getOutputActors() {
    return outputActors;
  }

  public OperatorType getRoleInChangedSubDag() {
    return roleInChangedSubDag;
  }

  public void setInputQueues(Map<ActorId, String> inputQueues) {
    this.inputQueues = inputQueues;
  }

  public void setInputActors(Map<String, RayActor> inputActors) {
    this.inputActors = inputActors;
  }

  public void setOutputQueues(Map<ActorId, String> outputQueues) {
    this.outputQueues = outputQueues;
  }

  public void setOutputActors(Map<String, RayActor> outputActors) {
    this.outputActors = outputActors;
  }

  public void setRoleInChangedSubDag(
      OperatorType roleInChangedSubDag) {
    this.roleInChangedSubDag = roleInChangedSubDag;
  }

  public ExecutionVertex withConfig(Map<String, Object> config) {
    executionConfig = new ExecutionConfig(new Configuration(config));
    executionConfig.setSubTaskIndex(subTaskIndex);
    executionConfig.setWorkerId(this.id.toString());

    // append job config
    updateJobConfig(exeJobVertex.getJobConf());

    return this;
  }

  public void updateJobConfig(Map<String, String> jobConfig) {
    // append job config
    if (jobConfig != null) {
      jobConfig.forEach((key, value)
          -> executionConfig.getConfiguration().setString(key, value));
    }
  }

  public boolean isToDelete() {
    return ExecutionVertexState.TO_DEL.equals(state);
  }

  public String getStreamName() {
    return this.getExeJobVertex().getJobVertex().getId().toString();
  }

  public ActorId getActorId() {
    return null == workerActor ? null : workerActor.getId();
  }

  public List<ExecutionVertex> getInputExecutionVertices() {
    return inputEdges.stream()
        .map(inputEdge -> inputEdge.getSource().getProducer())
        .collect(Collectors.toList());
  }

  public List<ExecutionVertex> getOutputExecutionVertices() {
    return resultPartitions.stream()
        .map(resultPartitions -> resultPartitions.getConsumers())
        .flatMap(Collection::stream)
        .map(outputEdge -> outputEdge.getTarget())
        .collect(Collectors.toList());
  }

  public boolean isSourceVertex() {
    return inputEdges.isEmpty();
  }

  public boolean isSinkVertex() {
    return resultPartitions.isEmpty();
  }

  // ----------------------------------------------------------------------
  // Rescaling relation methods
  // ----------------------------------------------------------------------

  /**
   * get indexed operator name, raw opName is like `SourceOperator\nnull\nSourceOperator`
   * indexed op name is like `1-SourceOperator`
   */
  public String getOpNameWithIndex() {
    OpInfo opInfo = getExeJobVertex().getJobVertex().getOpInfo();
    return String.format("%s-%s", opInfo.opIndex, opInfo.opName.split("\n")[0]).trim();
  }

  /**
   * Get OpInfo by split opName into tokens,  op name is like `1-SourceOperator`
   * @return OpInfo
   */
  public OpInfo getOpInfo() {
    String[] tokens = getOpNameWithIndex().split("-");
    return new OpInfo(Integer.parseInt(tokens[0]), tokens[1]);
  }
}
