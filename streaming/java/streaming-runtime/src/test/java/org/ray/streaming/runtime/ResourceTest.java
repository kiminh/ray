package org.ray.streaming.runtime;

import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import org.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import org.ray.streaming.runtime.core.resource.Container;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import org.ray.streaming.runtime.master.resourcemanager.ResourceManagerImpl;
import org.ray.streaming.runtime.master.scheduler.strategy.SlotAssignStrategy;
import org.ray.streaming.runtime.master.scheduler.strategy.impl.PipelineFirstStrategy;
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
  public void testMainFunction() {
    Ray.init();
    int expectedContainerNum = 5;
    ResourceManager resourceManager = new ResourceManagerImpl(jobMaster);

    // register container
    List<Container> containers = resourceManager.getRegisteredContainers();
    Assert.assertEquals(containers.size(), expectedContainerNum);

    // slot strategy
    SlotAssignStrategy strategy = resourceManager.getSlotAssignStrategy();
    Assert.assertEquals(strategy.getClass(), PipelineFirstStrategy.class);

    // slot number
    int slotNumPerContainer = strategy.getSlotNumPerContainer(containers,
        executionGraph.getMaxParallelism());
    List<ExecutionVertex> executionVertexList = executionGraph.getAllExecutionVertices();
    Assert.assertEquals(slotNumPerContainer,
        (int) Math.ceil(executionVertexList.size() * 1.0 / expectedContainerNum));

    // slot allocation
    strategy.allocateSlot(containers, slotNumPerContainer);
    containers.stream().forEach(container -> {
      Assert.assertEquals(container.getSlots().size(), slotNumPerContainer);
    });

    // slot assign
    Map<String, Map<Integer, List<String>>> allocatingMap = strategy.assignSlot(executionGraph);
    executionVertexList.stream().forEach(vertex -> {
      Assert.assertTrue(allocatingMap.toString().contains(vertex.getVertexName()));

      // resource allocation
      Map<String, Double> resources = resourceManager.allocateResource(vertex);
      Container container = resourceManager.getResources().getContainerByContainerId(
          vertex.getSlot().getContainerID());
      String containerName = container.getName();
      Assert.assertEquals(resources.get(containerName), 1.0);

      String containerAddress = container.getAddress();
      Assert.assertTrue(allocatingMap.get(containerAddress)
          .toString().contains(vertex.getVertexName()));
    });
  }
}
