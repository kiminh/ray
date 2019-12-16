package org.ray.streaming.runtime.core.graph.executiongraph;

import java.io.Serializable;
import java.util.Map;

import com.google.common.base.MoreObjects;

public class JobInformation implements Serializable {

  /** Job name */
  private final String jobName;
  private final Map<String, String> jobConf;

  public JobInformation(String jobName, Map<String, String> jobConf) {
    this.jobName = jobName;
    this.jobConf = jobConf;
  }

  public String getJobName() {
    return jobName;
  }

  public Map<String, String> getJobConf() {
    return jobConf;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobName", jobName)
        .add("jobConf", jobConf)
        .toString();
  }
}