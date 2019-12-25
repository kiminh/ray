package org.ray.streaming.runtime;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.Lists;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobGraphBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.DataStream;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.ray.streaming.runtime.util.TestHelper;

public class GraphTest {

  private static final Logger LOG = LoggerFactory.getLogger(GraphTest.class);

  private JobMaster jobMaster;

  @org.testng.annotations.BeforeClass
  public void setUp() {
    TestHelper.setUTPattern();
    Map<String, String> jobConfig = new HashMap<>();
    jobMaster = new JobMaster(jobConfig);
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTPattern();
  }

  @org.testng.annotations.BeforeMethod
  public void testBegin(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " begin >>>>>>>>>>>>>>>>>>");
  }

  @org.testng.annotations.AfterMethod
  public void testEnd(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " end >>>>>>>>>>>>>>>>>>");
  }

  @Test
  public void testBuildExecutionGraph() {
    GraphManager graphManager = new GraphManagerImpl(jobMaster);
    JobGraph jobGraph = buildJobGraph();
    ExecutionGraph executionGraph = buildExecutionGraph(graphManager, jobGraph);

    Assert.assertEquals(executionGraph.getExecutionJobVertexList().size(),
        jobGraph.getJobVertexList().size());

    int totalVertexNum = jobGraph.getJobVertexList().stream()
        .mapToInt(vertex -> vertex.getParallelism()).sum();
    Assert.assertEquals(executionGraph.getAllExecutionVertices().size(), totalVertexNum);
  }

  public static ExecutionGraph buildExecutionGraph(GraphManager graphManager) {
    return graphManager.buildExecutionGraph(buildJobGraph());
  }

  public static ExecutionGraph buildExecutionGraph(GraphManager graphManager, JobGraph jobGraph) {
    return graphManager.buildExecutionGraph(jobGraph);
  }

  public static JobGraph buildJobGraph() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    DataStream<String> dataStream = StreamSource.buildSource(streamingContext,
        Lists.newArrayList("a", "b", "c"));
    StreamSink streamSink = dataStream.sink(x -> LOG.info(x));
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(Lists.newArrayList(streamSink));

    JobGraph jobGraph = jobGraphBuilder.build();
    return jobGraph;
  }
}
