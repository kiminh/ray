package org.ray.yarn.config;

import org.apache.commons.cli.Options;
import org.apache.commons.lang.StringUtils;

/**
 * Configuration for Yarn Application Setup
 */
public class AppConfig extends AbstractConfig {

  // Application master specific info to register a new Application with RM/ASM
  String appName = "";
  // App master priority
  int amPriority = 0;
  // Queue for App master
  String amQueue = "";
  // Amt. of memory resource to request for to run the App Master
  long amMemory = 100;
  // Amt. of virtual core resource to request for to run the App Master
  int amVCores = 1;
  // log4j.properties file
  // if available, add to local resources and set into classpath
  String log4jPropFile = "";
  // Start time for client
  final long clientStartTime = System.currentTimeMillis();
  // Timeout threshold for client. Kill app after time interval expires.
  long clientTimeout = 600000;
  // flag to indicate whether to keep containers across application attempts.
  boolean keepContainers = false;
  // attempt failures validity interval
  long attemptFailuresValidityInterval = -1;
  // Debug flag
  boolean debugFlag = false;
  // Timeline domain ID
  String domainId = null;
  // Flag to indicate whether to create the domain of the given ID
  boolean toCreateDomain = false;
  // Timeline domain reader access control
  String viewAcls = null;
  // Timeline domain writer access control
  String modifyAcls = null;
  // The max times to get the 'RUNNING' state of application in monitor
  final int maxRunningStateTimes = 10;
  // Location of user archive resource
  String archiveResource = "";

  @Override
  public void validate() {
    // TODO

    if (amMemory < 0) {
      throw new IllegalArgumentException("Invalid memory specified for application master, exiting."
          + " Specified memory=" + amMemory);
    }
    if (amVCores < 0) {
      throw new IllegalArgumentException(
          "Invalid virtual cores specified for application master, exiting."
              + " Specified virtual cores=" + amVCores);
    }
  }

  public String getAppName() {
    return appName;
  }

  public void setAppName(String appName) {
    this.appName = appName;
  }

  public int getAmPriority() {
    return amPriority;
  }

  public void setAmPriority(int amPriority) {
    this.amPriority = amPriority;
  }

  public String getAmQueue() {
    return amQueue;
  }

  public void setAmQueue(String amQueue) {
    this.amQueue = amQueue;
  }

  public long getAmMemory() {
    return amMemory;
  }

  public void setAmMemory(long amMemory) {
    this.amMemory = amMemory;
  }

  public int getAmVCores() {
    return amVCores;
  }

  public void setAmVCores(int amVCores) {
    this.amVCores = amVCores;
  }

  public String getLog4jPropFile() {
    return log4jPropFile;
  }

  public void setLog4jPropFile(String log4jPropFile) {
    this.log4jPropFile = log4jPropFile;
  }

  public long getClientStartTime() {
    return clientStartTime;
  }

  public long getClientTimeout() {
    return clientTimeout;
  }

  public void setClientTimeout(long clientTimeout) {
    this.clientTimeout = clientTimeout;
  }

  public boolean isKeepContainers() {
    return keepContainers;
  }

  public void setKeepContainers(boolean keepContainers) {
    this.keepContainers = keepContainers;
  }

  public long getAttemptFailuresValidityInterval() {
    return attemptFailuresValidityInterval;
  }

  public void setAttemptFailuresValidityInterval(long attemptFailuresValidityInterval) {
    this.attemptFailuresValidityInterval = attemptFailuresValidityInterval;
  }

  public boolean isDebugFlag() {
    return debugFlag;
  }

  public void setDebugFlag(boolean debugFlag) {
    this.debugFlag = debugFlag;
  }

  public String getDomainId() {
    return domainId;
  }

  public void setDomainId(String domainId) {
    this.domainId = domainId;
  }

  public boolean isToCreateDomain() {
    return toCreateDomain;
  }

  public void setToCreateDomain(boolean toCreateDomain) {
    this.toCreateDomain = toCreateDomain;
  }

  public String getViewAcls() {
    return viewAcls;
  }

  public void setViewAcls(String viewAcls) {
    this.viewAcls = viewAcls;
  }

  public String getModifyAcls() {
    return modifyAcls;
  }

  public void setModifyAcls(String modifyAcls) {
    this.modifyAcls = modifyAcls;
  }

  public int getMaxRunningStateTimes() {
    return maxRunningStateTimes;
  }

  public String getArchiveResource() {
    return archiveResource;
  }

  public void setArchiveResource(String archiveResource) {
    this.archiveResource = archiveResource;
  }
}
