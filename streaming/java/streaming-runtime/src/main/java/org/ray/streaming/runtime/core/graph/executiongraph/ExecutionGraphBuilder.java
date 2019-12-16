package org.ray.streaming.runtime.core.graph.executiongraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;

import com.alipay.streaming.runtime.jobgraph.JobEdge;
import com.alipay.streaming.runtime.jobgraph.JobGraph;
import com.alipay.streaming.runtime.jobgraph.JobVertex;
import com.alipay.streaming.runtime.jobgraph.JobVertexID;
import com.alipay.streaming.runtime.utils.LoggerFactory;

public class ExecutionGraphBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(ExecutionGraphBuilder.class);

  public static ExecutionGraph buildGraph(JobGraph jobGraph) {
    LOG.info("Begin build execution graph with job graph {}.", jobGraph);

    ExecutionGraph executionGraph = new ExecutionGraph();
    Collection<JobVertex> jobVertices = jobGraph.getVertices();

    Map<JobVertexID, ExecutionJobVertex> exeJobVertexMap = new LinkedHashMap<>();
    for (JobVertex jobVertex : jobVertices) {
      exeJobVertexMap.put(jobVertex.getId(),
          new ExecutionJobVertex(jobVertex, executionGraph, jobGraph.getJobConfig()));
    }

    for (JobVertex jobVertex : jobVertices) {
      attachExecutionJobVertex(jobVertex, exeJobVertexMap);
    }

    for (ExecutionJobVertex exeJobVertex : exeJobVertexMap.values()) {
      exeJobVertex.attachExecutionVertex();
    }

    executionGraph.setExeJobVertices(exeJobVertexMap);

    List<ExecutionJobVertex> verticesInCreationOrder = new ArrayList<>(
        executionGraph.getExeJobVertices().values());
    executionGraph.setVerticesInCreationOrder(verticesInCreationOrder);
    int maxParallelism = jobVertices.stream().map(JobVertex::getParallelism)
        .max(Integer::compareTo).get();
    executionGraph.setMaxParallelism(maxParallelism);

    // set job information
    JobInformation jobInformation = new JobInformation(
        jobGraph.getJobName(),
        jobGraph.getJobConfig());
    executionGraph.setJobInformation(jobInformation);

    LOG.info("Build execution graph success.");
    return executionGraph;
  }

  private static void attachExecutionJobVertex(JobVertex jobVertex,
      Map<JobVertexID, ExecutionJobVertex> exeJobVertexMap) {
    ExecutionJobVertex exeJobVertex = exeJobVertexMap.get(jobVertex.getId());

    // get input edges
    List<JobEdge> inputs = jobVertex.getInputs();

    // attach execution job vertex
    for (JobEdge input : inputs) {
      JobVertex producer = input.getSource().getProducer();
      ExecutionJobVertex producerEjv = exeJobVertexMap.get(producer.getId());
      exeJobVertex.connectNewIntermediateResultAsInput(input, producerEjv);
    }
  }

}