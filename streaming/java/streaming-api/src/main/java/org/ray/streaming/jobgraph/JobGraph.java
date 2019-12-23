package org.ray.streaming.jobgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The logical execution plan.
 */
public class JobGraph implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobGraph.class);

  private final String jobName;
  private final Map<String, String> jobConfig;
  private final List<JobVertex> jobVertexList;
  private final List<JobEdge> jobEdgeList;

  public JobGraph(String jobName, Map<String, String> jobConfig) {
    this.jobName = jobName;
    this.jobConfig = jobConfig;
    this.jobVertexList = new ArrayList<>();
    this.jobEdgeList = new ArrayList<>();
  }

  public void addVertex(JobVertex vertex) {
    this.jobVertexList.add(vertex);
  }

  public void addEdge(JobEdge jobEdge) {
    this.jobEdgeList.add(jobEdge);
  }

  public List<JobVertex> getJobVertexList() {
    return jobVertexList;
  }

  public List<JobEdge> getJobEdgeList() {
    return jobEdgeList;
  }

  public String getGraphVizPlan() {
    return "";
  }

  public String getJobName() {
    return jobName;
  }

  public Map<String, String> getJobConfig() {
    return jobConfig;
  }

  public void printPlan() {
    if (!LOGGER.isInfoEnabled()) {
      return;
    }
    LOGGER.info("Printing logic plan:");
    for (JobVertex jobVertex : jobVertexList) {
      LOGGER.info(jobVertex.toString());
    }
    for (JobEdge jobEdge : jobEdgeList) {
      LOGGER.info(jobEdge.toString());
    }
  }
}
