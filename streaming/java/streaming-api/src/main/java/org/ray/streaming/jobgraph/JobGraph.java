package org.ray.streaming.jobgraph;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The logical execution plan.
 */
public class JobGraph implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(JobGraph.class);

  private Map<String, Object> jobConfig;
  private List<JobVertex> jobVertexList;
  private List<JobEdge> jobEdgeList;

  public JobGraph(Map<String, Object> jobConfig) {
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

  public Map<String, Object> getJobConfig() {
    return jobConfig;
  }
}
