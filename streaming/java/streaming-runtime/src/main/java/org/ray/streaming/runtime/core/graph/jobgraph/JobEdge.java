package org.ray.streaming.runtime.core.graph.jobgraph;

import java.io.Serializable;

public class JobEdge implements Serializable {
  private static final long serialVersionUID = 1L;

  private final IntermediateDataSet source;
  private final JobVertex target;
  private final DistributionPattern distributionPattern;

  public JobEdge(IntermediateDataSet source, JobVertex target,
      DistributionPattern distributionPattern) {
    this.source = source;
    this.target = target;
    this.distributionPattern = distributionPattern;
  }

  public IntermediateDataSet getSource() {
    return source;
  }

  public JobVertex getTarget() {
    return target;
  }

  public DistributionPattern getDistributionPattern() {
    return distributionPattern;
  }
}