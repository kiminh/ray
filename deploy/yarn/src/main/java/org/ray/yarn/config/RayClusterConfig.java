package org.ray.yarn.config;

import com.google.common.collect.Maps;
import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for Ray Cluster Setup
 */
public class RayClusterConfig extends AppConfig {

  // Amt of memory to request for container in which shell script will be executed
  long containerMemory = 10;
  // Amt. of virtual cores to request for container in which shell script will be executed
  int containerVCores = 1;
  // No. of containers in which the shell script needs to be executed
  int numContainers = 1;
  // Node Label to schedule
  String nodeLabelExpression = null;
  // supremeFo flag
  boolean supremeFo = false;
  // disable process failover flag
  boolean disableProcessFo = false;
  // Args to be passed to the shell command
  String[] shellArgs = new String[] {};
  // Env variables to be setup for the shell command
  Map<String, String> shellEnv = new HashMap<String, String>();
  // Shell Command Container priority
  int shellCmdPriority = 0;
  // Shell command to be executed
  // TODO different group have different shell commands
  String shellCommand = "";
  // No. of the Ray roles including head and work
  private Map<String, Integer> numRoles = Maps.newHashMapWithExpectedSize(2);

  @Override
  public void validate() {
    // TODO
    super.validate();

    // user defined env vars
    if (shellEnv != null && shellEnv.size() > 0) {
      String[] envs = shellArgs;
      for (String env : envs) {
        env = env.trim();
        int index = env.indexOf('=');
        if (index == -1) {
          shellEnv.put(env, "");
          continue;
        }
        String key = env.substring(0, index);
        String val = "";
        if (index < (env.length() - 1)) {
          val = env.substring(index + 1);
        }
        shellEnv.put(key, val);
      }
    }

    if (numRoles != null) {
      numRoles.put("head", 1);
      numRoles.put("work", 1);
    }

    if (containerMemory < 0 || containerVCores < 0 || numContainers < 1) {
      throw new IllegalArgumentException(
          "Invalid no. of containers or container memory/vcores specified," + " exiting."
              + " Specified containerMemory=" + containerMemory + ", containerVCores="
              + containerVCores + ", numContainer=" + numContainers);
    }

  }

  public long getContainerMemory() {
    return containerMemory;
  }

  public void setContainerMemory(long containerMemory) {
    this.containerMemory = containerMemory;
  }

  public int getContainerVCores() {
    return containerVCores;
  }

  public void setContainerVCores(int containerVCores) {
    this.containerVCores = containerVCores;
  }

  public int getNumContainers() {
    return numContainers;
  }

  public void setNumContainers(int numContainers) {
    this.numContainers = numContainers;
  }

  public String getNodeLabelExpression() {
    return nodeLabelExpression;
  }

  public void setNodeLabelExpression(String nodeLabelExpression) {
    this.nodeLabelExpression = nodeLabelExpression;
  }

  public boolean isSupremeFo() {
    return supremeFo;
  }

  public void setSupremeFo(boolean supremeFo) {
    this.supremeFo = supremeFo;
  }

  public boolean isDisableProcessFo() {
    return disableProcessFo;
  }

  public void setDisableProcessFo(boolean disableProcessFo) {
    this.disableProcessFo = disableProcessFo;
  }

  public String[] getShellArgs() {
    return shellArgs;
  }

  public void setShellArgs(String[] shellArgs) {
    this.shellArgs = shellArgs;
  }

  public Map<String, String> getShellEnv() {
    return shellEnv;
  }

  public void setShellEnv(Map<String, String> shellEnv) {
    this.shellEnv = shellEnv;
  }

  public int getShellCmdPriority() {
    return shellCmdPriority;
  }

  public void setShellCmdPriority(int shellCmdPriority) {
    this.shellCmdPriority = shellCmdPriority;
  }

  public String getShellCommand() {
    return shellCommand;
  }

  public void setShellCommand(String shellCommand) {
    this.shellCommand = shellCommand;
  }

  public Map<String, Integer> getNumRoles() {
    return numRoles;
  }

  public void setNumRoles(Map<String, Integer> numRoles) {
    this.numRoles = numRoles;
  }
}
