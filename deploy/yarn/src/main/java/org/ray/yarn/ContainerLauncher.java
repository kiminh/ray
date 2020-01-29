package org.ray.yarn;

import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ContainerManagementProtocol;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.URL;

/**
 * Thread to connect to the {@link ContainerManagementProtocol} and launch the container that will
 * execute the shell command.
 */
public class ContainerLauncher implements Runnable {

  private static final Log logger = LogFactory.getLog(ContainerLauncher.class);
  // Allocated container
  private Container container;
  private String rayInstanceId;
  private String role;
  private long sleepMillis = 0;

  NmCallbackHandler containerListener;

  public ContainerLauncher(Container lcontainer, NmCallbackHandler containerListener,
      String rayInstanceId, String role, long sleepMillis) {
    this.container = lcontainer;
    this.containerListener = containerListener;
    this.rayInstanceId = rayInstanceId;
    this.role = role;
    this.sleepMillis = sleepMillis;
  }

  @Override
  /**
   * Connects to CM, sets up container launch context for shell command and eventually dispatches
   * the container start request to the CM.
   */
  public void run() {
    logger.info("Setting up container launch container for containerid=" + container.getId()
        + " with rayInstanceId=" + rayInstanceId + " ,sleep millis " + sleepMillis);

    if (sleepMillis != 0) {
      try {
        Thread.sleep(sleepMillis);
      } catch (InterruptedException e) {
        logger.warn("Catch InterruptedException when sleep.");
      }

    }
    // Set the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    // The container for the eventual shell commands needs its own local
    // resources too.
    // In this scenario, if a shell script is specified, we need to have it
    // copied and made available to the container.
    String rayArchiveFileLocalPath = "Ray-package";
    if (!rayArchiveFile.isEmpty()) {
      Path rayArchivePath = new Path(rayArchiveFile);

      URL yarnUrl = null;
      try {
        yarnUrl = URL.fromURI(new URI(rayArchivePath.toString()));
      } catch (URISyntaxException e) {
        logger.error("Error when trying to use Ray archive path specified" + " in env, path="
            + rayArchivePath, e);
        // A failure scenario on bad input such as invalid shell script path
        // We know we cannot continue launching the container
        // so we should release it.
        // TODO
        numCompletedContainers.incrementAndGet();
        numFailedContainers.incrementAndGet();
        return;
      }
      LocalResource rayRsrc = LocalResource.newInstance(yarnUrl, LocalResourceType.ARCHIVE,
          LocalResourceVisibility.APPLICATION, rayArchiveFileLen, rayArchiveFileTimestamp);
      localResources.put(rayArchiveFileLocalPath, rayRsrc);
      shellCommand = LINUX_BASH_COMMEND;
    }

    // Set the necessary command to execute on the allocated container
    Vector<CharSequence> vargs = new Vector<CharSequence>(5);

    // Set executable command
    vargs.add(shellCommand);
    // Set shell script path
    if (!rayArchiveFile.isEmpty()) {
      vargs.add(rayArchiveFileLocalPath + "/" + rayShellStringPath);
    }

    // Set args based on role
    switch (role) {
      case "head":
        vargs.add("--head");
        vargs.add("--redis-address");
        vargs.add(redisAddress);
        if (headNodeStaticArgs != null) {
          vargs.add(headNodeStaticArgs);
        }
        break;
      case "work":
        //vargs.add("--work");
        vargs.add("--redis_port");
        vargs.add(String.valueOf(redisPort));
        if (workNodeStaticArgs != null) {
          vargs.add(workNodeStaticArgs);
        }
        break;
      default:
        break;
    }

    try {
      String nodeIpAddress =
          InetAddress.getByName(container.getNodeHttpAddress().split(":")[0]).getHostAddress();
      vargs.add("--node-ip-address");
      vargs.add(nodeIpAddress);
    } catch (UnknownHostException e) {
      e.printStackTrace();
    }

    // Set args for the shell command if any
    vargs.add(shellArgs);

    // Add log redirect params
    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());
    logger.info("command: " + commands);

    // Set up ContainerLaunchContext, setting local resource, environment,
    // command and token for constructor.

    Map<String, String> myShellEnv = new HashMap<String, String>(shellEnv);
    myShellEnv.put(YARN_SHELL_ID, rayInstanceId);
    ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(localResources, myShellEnv,
        commands, null, allTokens.duplicate(), null);
    containerListener.addContainer(container.getId(), container);
    nmClientAsync.startContainerAsync(container, ctx);
  }
}