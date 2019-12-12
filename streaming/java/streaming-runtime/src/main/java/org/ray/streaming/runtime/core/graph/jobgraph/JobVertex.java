package org.ray.streaming.runtime.core.graph.jobgraph;

import com.alipay.streaming.runtime.wrapper.StreamCollector;
import com.antfin.arc.arch.partition.IPartitioner;
import com.antfin.arch.engine.processor.Processor;
import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JobVertex implements Serializable {

  private static final long serialVersionUID = 1L;

  private final JobVertexID id;
  private final String name;
  private final List<IntermediateDataSet> results = new ArrayList<>();
  private final List<StreamCollector> outputs = new ArrayList<>();
  private final List<JobEdge> inputs = new ArrayList<>();
  private int parallelism;
  private Processor processor;
  private LanguageType languageType;
  private Map<String, Object> config = new HashMap<>();
  private final Map<String, Double> resources = new HashMap<>();

  public JobVertex(String name) {
    this.name = name;
    this.id = new JobVertexID();
  }

  public JobEdge connectNewDataSetAsInput(JobVertex input, IPartitioner partitioner) {
    IntermediateDataSet dataSet = input.createAndAddResultDataSet(partitioner);
    DistributionPattern distributionPattern = DistributionPattern.ALL_TO_ALL;
    // TODO: check which partitioner is POINTWISE DistributionPattern
    return connectDataSetAsInput(dataSet, distributionPattern);
  }

  private JobEdge connectDataSetAsInput(IntermediateDataSet dataSet,
      DistributionPattern distributionPattern) {
    JobEdge edge = new JobEdge(dataSet, this, distributionPattern);
    this.inputs.add(edge);
    dataSet.addConsumer(edge);
    return edge;
  }

  private IntermediateDataSet createAndAddResultDataSet(IPartitioner partitioner) {
    IntermediateDataSet result = new IntermediateDataSet(partitioner, this);
    this.results.add(result);
    return result;
  }

  public boolean isInputVertex() {
    return this.inputs.isEmpty();
  }

  public boolean isOutputVertex() {
    return this.results.isEmpty();
  }

  public void setParallelism(int parallelism) {
    this.parallelism = parallelism;
  }

  public void setProcessor(Processor processor) {
    this.processor = processor;
  }

  public void setLanguageType(LanguageType languageType) {
    this.languageType = languageType;
  }

  public void addOutput(StreamCollector output) {
    this.outputs.add(output);
  }

  public JobVertexID getId() {
    return id;
  }

  public String getName() {
    return name;
  }

  public List<IntermediateDataSet> getResults() {
    return results;
  }

  public List<JobEdge> getInputs() {
    return inputs;
  }

  public int getParallelism() {
    return parallelism;
  }

  public Processor getProcessor() {
    return processor;
  }

  public LanguageType getLanguageType() {
    return languageType;
  }

  public List<StreamCollector> getOutputs() {
    return outputs;
  }

  public Map<String, Object> getConfig() {
    return config;
  }

  public void setConfig(Map<String, Object> config) {
    this.config = config;
  }

  public Map<String, Double> getResources() {
    return resources;
  }

  public void setResources(Map<String, Double> resources) {
    this.resources.putAll(resources);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("name", name)
        .toString();
  }

  public static class OpInfo {

    public Integer opIndex;
    public String opName;

    public OpInfo(Integer opIndex, String opName) {
      this.opIndex = opIndex;
      this.opName = opName;
    }
  }

  public OpInfo getOpInfo() {
    String[] opInfos = getName().split("-");
    String opName = opInfos[1];
    Integer opIndex = Integer.parseInt(opInfos[0]);
    return new OpInfo(opIndex, opName);
  }
}