package org.ray.streaming.runtime.core.graph.jobgraph;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

public class JobGraph implements Serializable {
  private static final long serialVersionUID = 1L;

  private final String jobName;
  private final Map<String, String> jobConfig;
  private Map<JobVertexID, JobVertex> jobVertices = new LinkedHashMap<>();

  public JobGraph(String jobName, Map<String, String> jobConfig) {
    this.jobName = jobName;
    this.jobConfig = jobConfig;
  }

  public void addVertex(JobVertex vertex) {
    final JobVertexID id = vertex.getId();
    JobVertex previous = jobVertices.put(id, vertex);

    // if we had a prior association, restore and throw an exception
    if (previous != null) {
      jobVertices.put(id, previous);
      throw new IllegalArgumentException("The JobGraph already contains a vertex with that id.");
    }
  }

  public String getJobName() {
    return jobName;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public Collection<JobVertex> getVertices() {
    return jobVertices.values();
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("jobName", jobName)
        .toString();
  }
}