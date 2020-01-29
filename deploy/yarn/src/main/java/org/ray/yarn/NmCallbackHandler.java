package org.ray.yarn;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;

public class NmCallbackHandler extends NMClientAsync.AbstractCallbackHandler {

  public static final Log logger = LogFactory.getLog(RmCallbackHandler.class);

  private ConcurrentMap<ContainerId, Container> containers =
      new ConcurrentHashMap<ContainerId, Container>();
  private final ApplicationMaster applicationMaster;

  public NmCallbackHandler(ApplicationMaster applicationMaster) {
    this.applicationMaster = applicationMaster;
  }

  public void addContainer(ContainerId containerId, Container container) {
    containers.putIfAbsent(containerId, container);
  }

  @Override
  public void onContainerStopped(ContainerId containerId) {
    if (logger.isDebugEnabled()) {
      logger.debug("Succeeded to stop Container " + containerId);
    }
    containers.remove(containerId);
  }

  @Override
  public void onContainerStatusReceived(ContainerId containerId,
      ContainerStatus containerStatus) {
    if (logger.isDebugEnabled()) {
      logger.debug("Container Status: id=" + containerId + ", status=" + containerStatus);
    }
  }

  @Override
  public void onContainerStarted(ContainerId containerId,
      Map<String, ByteBuffer> allServiceResponse) {
    if (logger.isDebugEnabled()) {
      logger.debug("Succeeded to start Container " + containerId);
    }
    Container container = containers.get(containerId);
    if (container != null) {
      applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
    }
    if (applicationMaster.timelineClient != null) {
      applicationMaster.publishContainerStartEvent(applicationMaster.timelineClient, container,
          applicationMaster.domainId, applicationMaster.appSubmitterUgi);
    }
  }

  @Override
  public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {}

  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    logger.error("Failed to start Container " + containerId);
    containers.remove(containerId);
    applicationMaster.numCompletedContainers.incrementAndGet();
    applicationMaster.numFailedContainers.incrementAndGet();
  }

  @Override
  public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
    logger.error("Failed to query the status of Container " + containerId);
  }

  @Override
  public void onStopContainerError(ContainerId containerId, Throwable t) {
    logger.error("Failed to stop Container " + containerId);
    containers.remove(containerId);
  }

  @Override
  public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {}

}