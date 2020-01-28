package org.ray.yarn;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.ApplicationSubmissionContext;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.LocalResource;
import org.apache.hadoop.yarn.api.records.LocalResourceType;
import org.apache.hadoop.yarn.api.records.LocalResourceVisibility;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.URL;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.api.records.YarnClusterMetrics;
import org.apache.hadoop.yarn.api.records.timeline.TimelineDomain;
import org.apache.hadoop.yarn.client.api.TimelineClient;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.timeline.TimelineUtils;
import org.ray.yarn.config.RayClusterConfig;
import org.ray.yarn.utils.YamlUtil;


@InterfaceAudience.Public
@InterfaceStability.Unstable
public class Client {

  private static final Log logger = LogFactory.getLog(Client.class);

  // Configuration
  private Configuration conf;
  private YarnClient yarnClient;
  // Command line options
  private Options opts;

  // Application master jar file
  private String appMasterJar = "";
  // Main class to invoke application master
  private final String appMasterMainClass;

  private static final String appMasterJarPath = "AppMaster.jar";
  private static final String log4jPath = "log4j.properties";
  private static final String rayConfParam = "ray-conf";

  // Configuration
  RayClusterConfig rayConf = null;

  /**
   * The main entrance of Client.
   * 
   * @param args Command line arguments
   */
  public static void main(String[] args) {
    boolean result = false;
    try {
      Client client = new Client();
      logger.info("Initializing Client");
      try {
        boolean doRun = client.init(args);
        if (!doRun) {
          System.exit(0);
        }
      } catch (IllegalArgumentException e) {
        System.err.println(e.getLocalizedMessage());
        client.printUsage();
        System.exit(-1);
      }
      result = client.run();
    } catch (Throwable t) {
      logger.fatal("Error running Client", t);
      System.exit(1);
    }
    if (result) {
      logger.info("Application completed successfully");
      System.exit(0);
    }
    logger.error("Application failed to complete successfully");
    System.exit(2);
  }

  public Client(Configuration conf) throws Exception {
    this("org.ray.yarn.ApplicationMaster", conf);
  }

  Client(String appMasterMainClass, Configuration conf) {
    this.conf = conf;
    this.appMasterMainClass = appMasterMainClass;
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    opts = new Options();

    opts.addOption("help", false, "Print usage");
    opts.addOption(rayConfParam, true, "Ray Yarn Configuration");
  }

  public Client() throws Exception {
    this(new YarnConfiguration());
  }

  /**
   * Helper function to print out usage.
   */
  private void printUsage() {
    new HelpFormatter().printHelp("Client", opts);
  }

  /**
   * Parse command line options.
   * 
   * @param args Parsed command line options
   * @return Whether the init was successful to run the client
   */
  public boolean init(String[] args) throws ParseException {

    CommandLine cliParser = new GnuParser().parse(opts, args);

    if (args.length == 0) {
      throw new IllegalArgumentException("No args specified for client to initialize");
    }

    if (cliParser.hasOption("help")) {
      printUsage();
      return false;
    }

    if (!cliParser.hasOption(rayConfParam)) {
      throw new IllegalArgumentException(rayConfParam + " must be specified");
    }

    String rayConfPath = cliParser.getOptionValue(rayConfParam);
    try {
      rayConf = YamlUtil.loadFile(rayConfPath, RayClusterConfig.class);
      rayConf.validate();
    } catch (IOException e) {
      logger.error("Fail to read ray configuration from file " + rayConfPath);
    }

    // FIXME
    if (!cliParser.hasOption("jar")) {
      throw new IllegalArgumentException("No jar file specified for application master");
    }
    appMasterJar = cliParser.getOptionValue("jar");

    return true;
  }

  /**
   * Main run function for the client.
   * 
   * @return true if application completed successfully.
   */
  public boolean run() throws IOException, YarnException {

    logger.info("Running Client");
    yarnClient.start();

    YarnClusterMetrics clusterMetrics = yarnClient.getYarnClusterMetrics();
    logger.info("Got Cluster metric info from ASM" + ", numNodeManagers="
        + clusterMetrics.getNumNodeManagers());

    List<NodeReport> clusterNodeReports = yarnClient.getNodeReports(NodeState.RUNNING);
    logger.info("Got Cluster node info from ASM");
    for (NodeReport node : clusterNodeReports) {
      logger.info("Got node report from ASM for" + ", nodeId=" + node.getNodeId() + ", nodeAddress="
          + node.getHttpAddress() + ", nodeRackName=" + node.getRackName() + ", nodeNumContainers="
          + node.getNumContainers());
    }

    QueueInfo queueInfo = yarnClient.getQueueInfo(rayConf.getAmQueue());
    if (queueInfo != null) {
      logger.info("Queue info" + ", queueName=" + queueInfo.getQueueName() + ", queueCurrentCapacity="
          + queueInfo.getCurrentCapacity() + ", queueMaxCapacity=" + queueInfo.getMaximumCapacity()
          + ", queueApplicationCount=" + queueInfo.getApplications().size()
          + ", queueChildQueueCount=" + queueInfo.getChildQueues().size());
    }

    List<QueueUserACLInfo> listAclInfo = yarnClient.getQueueAclsInfo();
    for (QueueUserACLInfo aclInfo : listAclInfo) {
      for (QueueACL userAcl : aclInfo.getUserAcls()) {
        logger.info("User ACL Info for Queue" + ", queueName=" + aclInfo.getQueueName() + ", userAcl="
            + userAcl.name());
      }
    }

    if (StringUtils.isNotBlank(rayConf.getDomainId()) && rayConf.isToCreateDomain()) {
      prepareTimelineDomain();
    }

    // Get a new application id
    YarnClientApplication app = yarnClient.createApplication();
    GetNewApplicationResponse appResponse = app.getNewApplicationResponse();

    long maxMem = appResponse.getMaximumResourceCapability().getMemorySize();
    // TODO get min/max resource capabilities from RM and change memory ask if needed
    // If we do not have min/max, we may not be able to correctly request 
    // the required resources from the RM for the app master
    // Memory ask has to be a multiple of min and less than max. 
    // Dump out information about cluster capability as seen by the resource manager
    logger.info("Max mem capability of resources in this cluster " + maxMem);
    // A resource ask cannot exceed the maxMem.
    if (rayConf.getAmMemory() > maxMem) {
      logger.info("AM memory specified above max threshold of cluster. Using max value."
          + ", specified=" + rayConf.getAmMemory() + ", max=" + maxMem);
      rayConf.setAmMemory(maxMem);
    }

    // A resource ask cannot exceed the maxVCores.
    int maxVCores = appResponse.getMaximumResourceCapability().getVirtualCores();
    logger.info("Max virtual cores capability of resources in this cluster " + maxVCores);
    if (rayConf.getAmVCores() > maxVCores) {
      logger.info("AM virtual cores specified above max threshold of cluster. " + "Using max value."
          + ", specified=" + rayConf.getAmVCores() + ", max=" + maxVCores);
      rayConf.setAmVCores(maxVCores);
    }

    // set the application name
    ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
    appContext.setKeepContainersAcrossApplicationAttempts(rayConf.isKeepContainers());
    appContext.setApplicationName(rayConf.getAppName());

    if (rayConf.getAttemptFailuresValidityInterval() >= 0) {
      appContext.setAttemptFailuresValidityInterval(rayConf.getAttemptFailuresValidityInterval());
    }

    // set local resources for the application master
    // local files or archives as needed
    // In this scenario, the jar file for the application master is part of the local resources
    Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();

    logger.info("Copy App Master jar from local filesystem and add to local environment");
    // Copy the application master jar to the filesystem
    // Create a local resource to point to the destination jar path
    FileSystem fs = FileSystem.get(conf);
    ApplicationId appId = appContext.getApplicationId();
    addToLocalResources(fs, appMasterJar, appMasterJarPath, appId.toString(), localResources, null);

    // Set the log4j properties if needed
    if (!rayConf.getLog4jPropFile().isEmpty()) {
      addToLocalResources(fs, rayConf.getLog4jPropFile(), log4jPath, appId.toString(), localResources, null);
    }

    // The shell script has to be made available on the final container(s)
    // where it will be executed.
    // To do this, we need to first copy into the filesystem that is visible
    // to the yarn framework.
    // We do not need to set this as a local resource for the application
    // master as the application master does not need it.
    String hdfsArchiveLocation = "";
    long hdfsArchiveLen = 0;
    long hdfsArchiveTimestamp = 0;
    if (!rayConf.getArchiveResource().isEmpty()) {
      Path raySrc = new Path(rayConf.getArchiveResource());
      String rayPathSuffix = rayConf.getAppName() + "/" + appId.toString() + "/" + rayConf.getArchiveResource();
      Path rayDst = new Path(fs.getHomeDirectory(), rayPathSuffix);
      fs.copyFromLocalFile(false, true, raySrc, rayDst);
      hdfsArchiveLocation = fs.getHomeDirectory() + "/" + rayPathSuffix;
      FileStatus rayArchiveFileStatus = fs.getFileStatus(rayDst);
      hdfsArchiveLen = rayArchiveFileStatus.getLen();
      hdfsArchiveTimestamp = rayArchiveFileStatus.getModificationTime();
    }

    // FIXME
//    if (!shellCommand.isEmpty()) {
//      addToLocalResources(fs, null, shellCommandPath, appId.toString(), localResources,
//          shellCommand);
//    }
//    if (shellArgs.length > 0) {
//      addToLocalResources(fs, null, shellArgsPath, appId.toString(), localResources,
//          StringUtils.join(shellArgs, " "));
//    }

    // Set the necessary security tokens as needed
    // amContainer.setContainerTokens(containerToken);

    // Set the env variables to be setup in the env where the application master will be run
    logger.info("Set the environment for the application master");
    Map<String, String> env = new HashMap<String, String>();
    // put location of shell script into env
    // using the env info, the application master will create the correct local resource for the
    // eventual containers that will be launched to execute the shell scripts
    env.put(DsConstants.RAY_ARCHIVE_LOCATION, hdfsArchiveLocation);
    env.put(DsConstants.RAY_ARCHIVE_TIMESTAMP, Long.toString(hdfsArchiveTimestamp));
    env.put(DsConstants.RAY_ARCHIVE_LEN, Long.toString(hdfsArchiveLen));
    if (StringUtils.isNotBlank(rayConf.getDomainId())) {
      env.put(DsConstants.RAY_TIMELINE_DOMAIN, rayConf.getDomainId());
    }

    // Add AppMaster.jar location to classpath
    // At some point we should not be required to add
    // the hadoop specific classpaths to the env.
    // It should be provided out of the box.
    // For now setting all required classpaths including
    // the classpath to "." for the application jar
    StringBuilder classPathEnv = new StringBuilder(Environment.CLASSPATH.$$())
        .append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
    for (String c : conf.getStrings(YarnConfiguration.YARN_APPLICATION_CLASSPATH,
        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
      classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
      classPathEnv.append(c.trim());
    }
    classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log4j.properties");
    // add the runtime classpath needed for tests to work
    if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
      classPathEnv.append(':');
      classPathEnv.append(System.getProperty("java.class.path"));
    }
    env.put("CLASSPATH", classPathEnv.toString());

    // Set the necessary command to execute the application master
    Vector<CharSequence> vargs = new Vector<CharSequence>(30);

    // Set java executable command
    logger.info("Setting up app master command");
    vargs.add(Environment.JAVA_HOME.$$() + "/bin/java");
    // Set Xmx based on am memory size
    vargs.add("-Xmx" + rayConf.getAmMemory() + "m");;

    // Set class name
    vargs.add(appMasterMainClass);
    // Set params for Application Master
    vargs.add("--containerMemory " + String.valueOf(rayConf.getContainerMemory()));
    vargs.add("--containerVcores " + String.valueOf(rayConf.getContainerVirtualCores()));
    vargs.add("--numContainers " + String.valueOf(rayConf.getNumContainers()));
    // TODO replace with conf file

    vargs.add("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stdout");
    vargs.add("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/AppMaster.stderr");

    // Get final commmand
    StringBuilder command = new StringBuilder();
    for (CharSequence str : vargs) {
      command.append(str).append(" ");
    }

    logger.info("Completed setting up app master command " + command.toString());
    List<String> commands = new ArrayList<String>();
    commands.add(command.toString());

    // Set up the container launch context for the application master
    ContainerLaunchContext amContainer =
        ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

    // Set up resource type requirements
    // For now, both memory and vcores are supported, so we set memory and
    // vcores requirements
    Resource capability = Resource.newInstance(rayConf.getAmMemory(), rayConf.getAmVCores());
    appContext.setResource(capability);

    // Service data is a binary blob that can be passed to the application
    // Not needed in this scenario
    // amContainer.setServiceData(serviceData);

    // Setup security tokens
    if (UserGroupInformation.isSecurityEnabled()) {
      // Note: Credentials class is marked as LimitedPrivate for HDFS and MapReduce
      Credentials credentials = new Credentials();
      String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
      if (tokenRenewer == null || tokenRenewer.length() == 0) {
        throw new IOException("Can't get Master Kerberos principal for the RM to use as renewer");
      }

      // For now, only getting tokens for the default file-system.
      final Token<?>[] tokens = fs.addDelegationTokens(tokenRenewer, credentials);
      if (tokens != null) {
        for (Token<?> token : tokens) {
          logger.info("Got dt for " + fs.getUri() + "; " + token);
        }
      }
      DataOutputBuffer dob = new DataOutputBuffer();
      credentials.writeTokenStorageToStream(dob);
      ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
      amContainer.setTokens(fsTokens);
    }

    appContext.setAMContainerSpec(amContainer);

    // Set the priority for the application master
    Priority pri = Priority.newInstance(rayConf.getAmPriority());
    appContext.setPriority(pri);

    // Set the queue to which this application is to be submitted in the RM
    appContext.setQueue(rayConf.getAmQueue());

    // Submit the application to the applications manager
    // SubmitApplicationResponse submitResp = applicationsManager.submitApplication(appRequest);
    // Ignore the response as either a valid response object is returned on success
    // or an exception thrown to denote some form of a failure
    logger.info("Submitting application to ASM");

    yarnClient.submitApplication(appContext);

    // TODO
    // Try submitting the same request again
    // app submission failure?

    // Monitor the application
    return monitorApplication(appId);
  }

  /**
   * Monitor the submitted application for completion. Kill application if time expires.
   * 
   * @param appId Application Id of application to be monitored
   * @return true if application completed successfully
   */
  private boolean monitorApplication(ApplicationId appId) throws YarnException, IOException {

    int runningStateTimes = 0;
    while (true) {

      // Check app status every 1 second.
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        logger.debug("Thread sleep in monitoring loop interrupted");
      }

      // Get application report for the appId we are interested in
      ApplicationReport report = yarnClient.getApplicationReport(appId);

      logger.info("Got application report from ASM for" + ", appId=" + appId.getId()
          + ", clientToAMToken=" + report.getClientToAMToken() + ", appDiagnostics="
          + report.getDiagnostics() + ", appMasterHost=" + report.getHost() + ", appQueue="
          + report.getQueue() + ", appMasterRpcPort=" + report.getRpcPort() + ", appStartTime="
          + report.getStartTime() + ", yarnAppState=" + report.getYarnApplicationState().toString()
          + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
          + ", appTrackingUrl=" + report.getTrackingUrl() + ", appUser=" + report.getUser());

      YarnApplicationState state = report.getYarnApplicationState();
      FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();
      if (YarnApplicationState.FINISHED == state) {
        if (FinalApplicationStatus.SUCCEEDED == dsStatus) {
          logger.info("Application has completed successfully. Breaking monitoring loop");
          return true;
        } else {
          logger.info("Application did finished unsuccessfully." + " YarnState=" + state.toString()
              + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
          return false;
        }
      } else if (YarnApplicationState.KILLED == state || YarnApplicationState.FAILED == state) {
        logger.info("Application did not finish." + " YarnState=" + state.toString()
            + ", DSFinalStatus=" + dsStatus.toString() + ". Breaking monitoring loop");
        return false;
      }

      if (System.currentTimeMillis() > (rayConf.getClientStartTime() + rayConf.getClientTimeout())) {
        logger.info("Reached client specified timeout for application. Killing application");
        forceKillApplication(appId);
        return false;
      }

      if (YarnApplicationState.RUNNING == state && rayConf.getMaxRunningStateTimes() > 0) {
        runningStateTimes++;
        if (runningStateTimes >= rayConf.getMaxRunningStateTimes()) {
          logger.info("App state has been 'RUNNING' for " + rayConf.getMaxRunningStateTimes()
              + " times. Breaking monitoring loop");
          logger.info("APP_TRACKING_URL=" + report.getTrackingUrl());
          logger.info("APP_ORIGINAL_TRACKING_URL=" + report.getOriginalTrackingUrl());
          return true;
        }
      } else {
        runningStateTimes = 0;
      }
    }

  }

  /**
   * Kill a submitted application by sending a call to the ASM.
   * 
   * @param appId Application Id to be killed.
   */
  private void forceKillApplication(ApplicationId appId) throws YarnException, IOException {
    // TODO clarify whether multiple jobs with the same app id can be submitted and be running at
    // the same time.
    // If yes, can we kill a particular attempt only?

    // Response can be ignored as it is non-null on success or
    // throws an exception in case of failures
    yarnClient.killApplication(appId);
  }

  private void addToLocalResources(FileSystem fs, String fileSrcPath, String fileDstPath,
      String appId, Map<String, LocalResource> localResources, String resources)
      throws IOException {
    String suffix = rayConf.getAppName() + "/" + appId + "/" + fileDstPath;
    Path dst = new Path(fs.getHomeDirectory(), suffix);
    if (fileSrcPath == null) {
      FSDataOutputStream ostream = null;
      try {
        ostream = FileSystem.create(fs, dst, new FsPermission((short) 0710));
        ostream.writeUTF(resources);
      } finally {
        IOUtils.closeQuietly(ostream);
      }
    } else {
      fs.copyFromLocalFile(new Path(fileSrcPath), dst);
    }
    FileStatus scFileStatus = fs.getFileStatus(dst);
    LocalResource scRsrc = LocalResource.newInstance(URL.fromURI(dst.toUri()),
        LocalResourceType.FILE, LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
        scFileStatus.getModificationTime());
    localResources.put(fileDstPath, scRsrc);
  }

  private void prepareTimelineDomain() {
    TimelineClient timelineClient = null;
    if (conf.getBoolean(YarnConfiguration.TIMELINE_SERVICE_ENABLED,
        YarnConfiguration.DEFAULT_TIMELINE_SERVICE_ENABLED)) {
      timelineClient = TimelineClient.createTimelineClient();
      timelineClient.init(conf);
      timelineClient.start();
    } else {
      logger.warn(
          "Cannot put the domain " + rayConf.getDomainId() + " because the timeline service is not enabled");
      return;
    }
    try {
      // TODO: we need to check and combine the existing timeline domain ACLs,
      // but let's do it once we have client java library to query domains.
      TimelineDomain domain = new TimelineDomain();
      domain.setId(rayConf.getDomainId());
      domain.setReaders(
          rayConf.getViewAcls() != null && rayConf.getViewAcls().length() > 0 ? rayConf.getViewAcls() : " ");
      domain.setWriters(
          rayConf.getModifyAcls() != null && rayConf.getModifyAcls().length() > 0 ? rayConf.getModifyAcls() : " ");
      timelineClient.putDomain(domain);
      logger.info("Put the timeline domain: " + TimelineUtils.dumpTimelineRecordtoJSON(domain));
    } catch (Exception e) {
      logger.error("Error when putting the timeline domain", e);
    } finally {
      timelineClient.stop();
    }
  }
}
