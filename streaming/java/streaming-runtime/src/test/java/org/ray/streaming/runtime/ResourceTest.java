package org.ray.streaming.runtime;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManagerImpl;
import org.ray.streaming.runtime.util.TestHelper;

public class ResourceTest {

  private static final Logger LOG = LoggerFactory.getLogger(ResourceTest.class);

  private JobMaster jobMaster;
  private ExecutionGraph executionGraph;

  @org.testng.annotations.BeforeClass
  public void setUp() {
    TestHelper.setUTPattern();
    Map<String, String> jobConfig = new HashMap<>();
    jobMaster = new JobMaster(jobConfig);
    executionGraph = GraphTest.buildExecutionGraph(new GraphManagerImpl(jobMaster));
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
  public void testAllocateResource() {
    ResourceManager resourceManager = new ResourceManagerImpl(jobMaster);
    System.out.println(resourceManager.getRegisteredContainers());

    List<ExecutionVertex> executionVertexList = executionGraph.getAllExecutionVertices();
//    executionVertexList.stream().forEach(vertex -> {
//      Map<String, Double> allocatedResource = resourceManager.allocateResource(vertex);
//      Assert.assertTrue();
//    });

  }
}
