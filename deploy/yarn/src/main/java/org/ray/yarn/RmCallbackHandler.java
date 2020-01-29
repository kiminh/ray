package org.ray.yarn;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.UpdatedContainer;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

public class RmCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {

  public static final Log logger = LogFactory.getLog(RmCallbackHandler.class);

  @Override
  public void onContainersCompleted(List<ContainerStatus> completedContainers) {
    Boolean restartClasterFlag = false;
    logger.info(
        "Got response from RM for container ask, completedCnt=" + completedContainers.size());
    for (ContainerStatus containerStatus : completedContainers) {
      logger.info(appAttemptId + " got container status for containerID="
          + containerStatus.getContainerId() + ", state=" + containerStatus.getState()
          + ", exitStatus=" + containerStatus.getExitStatus() + ", diagnostics="
          + containerStatus.getDiagnostics());

      // non complete containers should not be here
      assert (containerStatus.getState() == ContainerState.COMPLETE);
      // ignore containers we know nothing about - probably from a previous
      // attempt
      if (!launchedContainers.contains(containerStatus.getContainerId())) {
        logger.info("Ignoring completed status of " + containerStatus.getContainerId()
            + "; unknown container(probably launched by previous attempt)");
        continue;
      }

      // increment counters for completed/failed containers
      int exitStatus = containerStatus.getExitStatus();
      if (0 != exitStatus) {
        // container failed
        logger.info("container failed, exit status is " + exitStatus);
        for (RayNodeContext node : indexToNode) {
          if (node.container != null
              && node.container.getId().equals(containerStatus.getContainerId())) {
            logger.info("ray node failed, the role is " + node.role);
            if (-100 == exitStatus) { /* release container will return -100 */
              if (node.isRunning == false) {
                logger.info("release container will return -100, don't process it");
                break;
              } else {
                logger.warn("the exit status is -100, but this node should be running");
              }
            }
            node.isRunning = false;
            node.isAllocating = false;
            node.instanceId = null;
            node.container = null;
            node.failCounter++;

            if (disableProcessFo) {
              logger.info("process failover is disable, ignore container failed");
              break;
            }

            if (supremeFo) {
              logger.info("Start supreme failover");
              restartClasterFlag = true;
            }

            if (node.role == "head") {
              restartClasterFlag = true;
            }
            numAllocatedContainers.decrementAndGet();
            numRequestedContainers.decrementAndGet();
            break;
          }
        }

        if (restartClasterFlag) {
          logger.info("restart all the Container of ray node");
          for (RayNodeContext node : indexToNode) {
            if (node.isRunning && node.container != null) {
              amRmClient.releaseAssignedContainer(node.container.getId());
              node.isRunning = false;
              node.isAllocating = false;
              node.instanceId = null;
              node.container = null;
              node.failCounter++;
              numAllocatedContainers.decrementAndGet();
              numRequestedContainers.decrementAndGet();
            }
          }
        }
      } else {
        // nothing to do
        // container completed successfully
        numCompletedContainers.incrementAndGet();
        logger.info("Container completed successfully." + ", containerId="
            + containerStatus.getContainerId());
      }
      if (timelineClient != null) {
        publishContainerEndEvent(timelineClient, containerStatus, domainId, appSubmitterUgi);
      }

      if (restartClasterFlag) {
        break;
      }
    }

    // ask for more containers if any failed
    int askCount = numTotalContainers - numRequestedContainers.get();
    numRequestedContainers.addAndGet(askCount);

    int requestCount = setupContainerRequest();
    assert requestCount == askCount : "The request count is inconsistent(onContainersCompleted): "
        + requestCount + " != " + askCount;

    if (numCompletedContainers.get() == numTotalContainers) {
      done = true;
    }
  }

  @Override
  public void onContainersAllocated(List<Container> allocatedContainers) {
    logger.info(
        "Got response from RM for container ask, allocatedCnt=" + allocatedContainers.size());
    numAllocatedContainers.addAndGet(allocatedContainers.size());
    for (Container allocatedContainer : allocatedContainers) {
      String rayInstanceId = Integer.toString(rayInstanceCounter);
      rayInstanceCounter++;

      Thread launchThread = null;
      boolean shouldSleep = false;
      for (RayNodeContext node : indexToNode) {
        if (node.isRunning) {
          continue;
        }
        node.isRunning = true;
        node.isAlocating = false;
        node.instanceId = rayInstanceId;
        node.container = allocatedContainer;
        containerToNode.put(allocatedContainer.getId().toString(), node);
        if (node.role == "head") {
          try {
            redisAddress =
                InetAddress.getByName(allocatedContainer.getNodeHttpAddress().split(":")[0])
                    .getHostAddress() + ":" + redisPort;
          } catch (UnknownHostException e) {
            redisAddress = "";
          }
        } else {
          shouldSleep = true;
        }
        launchThread = createLaunchContainerThread(allocatedContainer, rayInstanceId, node.role,
            shouldSleep ? 20000 : 0);
        break;
      }

      if (launchThread == null) {
        logger.error("The container " + allocatedContainer + " unused!");
        break;
      }

      logger.info("Launching Ray instance on a new container." + ", containerId="
          + allocatedContainer.getId() + ", rayInstanceId=" + rayInstanceId + ", containerNode="
          + allocatedContainer.getNodeId().getHost() + ":"
          + allocatedContainer.getNodeId().getPort() + ", containerNodeURI="
          + allocatedContainer.getNodeHttpAddress() + ", containerResourceMemory="
          + allocatedContainer.getResource().getMemorySize() + ", containerResourceVirtualCores="
          + allocatedContainer.getResource().getVirtualCores());
      // + ", containerToken"
      // +allocatedContainer.getContainerToken().getIdentifier().toString());

      // launch and start the container on a separate thread to keep
      // the main thread unblocked
      // as all containers may not be allocated at one go.
      launchThreads.add(launchThread);
      launchedContainers.add(allocatedContainer.getId());
      launchThread.start();
    }
  }

  @Override
  public void onContainersUpdated(List<UpdatedContainer> containers) {
  }

  @Override
  public void onShutdownRequest() {
    done = true;
  }

  @Override
  public void onNodesUpdated(List<NodeReport> updatedNodes) {
  }

  @Override
  public float getProgress() {
    // set progress to deliver to RM on next heartbeat
    float progress = (float) numCompletedContainers.get() / numTotalContainers;
    return progress;
  }

  @Override
  public void onError(Throwable e) {
    logger.error("Error in RMCallbackHandler: ", e);
    done = true;
    amRmClient.stop();
  }
}
