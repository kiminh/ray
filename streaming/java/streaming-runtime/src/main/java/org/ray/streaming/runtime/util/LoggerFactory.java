package org.ray.streaming.runtime.util;

import java.io.File;
import java.lang.management.ManagementFactory;

import org.aeonbits.owner.ConfigFactory;
import org.apache.commons.lang.StringUtils;
import org.slf4j.ILoggerFactory;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.global.LogConfig;
import org.ray.streaming.runtime.util.logger.Log4j2Utils;
import org.ray.streaming.runtime.util.logger.Log4jUtils;

public class LoggerFactory {

  private static final String LOGGER_FACTORY_LOG4J = "org.slf4j.impl.Log4jLoggerFactory";
  private static final String LOGGER_FACTORY_LOG4J2 = "org.apache.logging.slf4j.Log4jLoggerFactory";
  private static final LogConfig LOG_CONFIG = ConfigFactory.create(LogConfig.class);

  public static String getLogFileName() {
    String logDir = System.getenv(LOG_CONFIG.logDirEnvKey());
    if (null == logDir) {
      logDir = LOG_CONFIG.logRootDir();
    }

    File dir = new File(logDir);
    if (!dir.exists()) {
      dir.mkdirs();
    }

    String logFile = logDir + File.separator + LOG_CONFIG.logFilePattern();

    String pid = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
    return String.format(logFile, pid);
  }

  public static Logger getLogger(Class clazz) {
    ILoggerFactory iLoggerFactory = org.slf4j.LoggerFactory.getILoggerFactory();

    if (LOGGER_FACTORY_LOG4J.equals(iLoggerFactory.getClass().getName())) {
      //log4j
      Log4jUtils.initLog4jLogger(clazz, getLogFileName(), getMaxBackupIndex(), getMaxFileSize(),
          getLogLevel());
    } else if (LOGGER_FACTORY_LOG4J2.equals(iLoggerFactory.getClass().getName())) {
      //log4j2
      Log4j2Utils.initLog4j2Logger(clazz, getLogFileName(), getMaxBackupIndex(), getMaxFileSize(),
          getLogLevel());
    } else {
      throw new RuntimeException("Unsupported logger factory " + iLoggerFactory + "!");
    }
    return iLoggerFactory.getLogger(clazz.getName());
  }


  private static String getLogLevel() {
    String logLevelStr = System.getenv(LOG_CONFIG.logLevelEnvKey());
    if (StringUtils.isEmpty(logLevelStr)) {
      logLevelStr = LOG_CONFIG.logLevel();
    }
    return logLevelStr;
  }

  private static int getMaxBackupIndex() {
    String maxBackupCountStr = System.getenv(LOG_CONFIG.logBackupCountEnvKey());
    if (StringUtils.isEmpty(maxBackupCountStr)) {
      return LOG_CONFIG.logBackupCount();
    }
    return Integer.valueOf(maxBackupCountStr);
  }

  private static long getMaxFileSize() {
    String maxFileSizeStr = System.getenv(LOG_CONFIG.logMaxBytesEnvKey());
    if (StringUtils.isEmpty(maxFileSizeStr)) {
      return LOG_CONFIG.logMaxBytes();
    }
    return Long.valueOf(maxFileSizeStr);
  }

}