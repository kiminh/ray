package org.ray.streaming.runtime.core.resource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.alipay.arc.deploy.clusterapiserver.model.ElasticityContainer;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;

public class ContainerAllocationState {

  private List<ElasticityContainer> elasticityContainers = new ArrayList<>();
  private int allocatedIndex = 0;
  private volatile boolean allocationFinished = true;
  private long deployStartTimestamp;
  private int deployTimeoutSecs;

  public ContainerAllocationState() {
  }

  public ContainerAllocationState(ElasticityContainer[] elasticityContainers, int deployTimeoutSecs) {
    Preconditions.checkState(elasticityContainers.length > 0);
    Preconditions.checkState(deployTimeoutSecs > 0);
    this.elasticityContainers = Arrays.asList(elasticityContainers);
    this.allocationFinished = false;
    this.deployStartTimestamp = System.currentTimeMillis();
    this.deployTimeoutSecs = deployTimeoutSecs;
  }

  public void incAllocatedIndex() {
    this.allocatedIndex++;
    if (this.allocatedIndex == elasticityContainers.size()) {
      this.allocationFinished = true;
    }
  }

  public boolean isAllocationFinished() {
    return allocationFinished;
  }

  public boolean keepWaiting() {
    return !isDeployTimeout() && !isAllocationFinished();
  }

  public boolean isDeployTimeout() {
    return (System.currentTimeMillis() - deployStartTimestamp) > deployTimeoutSecs * 1000;
  }

  public ElasticityContainer nextContainer() {
    if (allocationFinished) {
      return null;
    }
    return elasticityContainers.get(allocatedIndex);
  }

  public int waitingNum() {
    return elasticityContainers.size() - allocatedIndex;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("elasticityContainers", elasticityContainers)
        .add("allocatedIndex", allocatedIndex)
        .add("allocationFinished", allocationFinished)
        .toString();
  }
}
