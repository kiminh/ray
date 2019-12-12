package org.ray.streaming.runtime.config.global;

import org.aeonbits.owner.Config;

/**
 *
 */
public interface CommonConfig extends Config {

  String JOB_NAME = "streaming.job.name";
  String PY_JOB_NAME = "job_name";
  String FILE_ENCODING = "streaming.file.encoding";
  String FILE_SUFFIX_ZIP = "streaming.file.zip.suffix";
  String FILE_SUFFIX_JAR = "streaming.file.jar.suffix";
  String FILE_SUFFIX_CLASS = "streaming.file.class.suffix";

  /**
   * Streaming job name.
   * @return
   */
  @DefaultValue(value = "default-job-name")
  @Key(value = JOB_NAME)
  String jobName();

  /**
   * Standard file encoding type in streaming.
   * @return
   */
  @DefaultValue(value = "UTF-8")
  @Key(value = FILE_ENCODING)
  String fileEncoding();

  /**
   * Standard suffix character for zip file in streaming.
   * @return
   */
  @DefaultValue(value = ".zip")
  @Key(value = FILE_SUFFIX_ZIP)
  String zipFileSuffix();

  /**
   * Standard suffix character for jar file in streaming.
   * @return
   */
  @DefaultValue(value = ".jar")
  @Key(value = FILE_SUFFIX_JAR)
  String jarFileSuffix();

  /**
   * Standard suffix character for class file in streaming.
   * @return
   */
  @DefaultValue(value = ".class")
  @Key(value = FILE_SUFFIX_CLASS)
  String classFileSuffix();
}
