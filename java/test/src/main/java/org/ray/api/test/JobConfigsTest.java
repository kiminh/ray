package org.ray.api.test;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.TestUtils;
import org.ray.api.annotation.RayRemote;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class JobConfigsTest extends BaseTest {

  @BeforeClass
  public void setupJobConfigs() {
    System.setProperty("ray.job.num-java-workers-per-process", "3");
    System.setProperty("ray.job.jvm-options.0", "-DX=999");
  }

  @AfterClass
  public void tearDownJobConfigs() {
    System.clearProperty("ray.job.num-java-workers-per-process");
    System.clearProperty("ray.job.jvm-options.0");
  }

  @RayRemote
  public static String getJvmOptions() {
    return System.getProperty("X");
  }

  @RayRemote
  public static Integer getWorkersNum() {
    return TestUtils.getRuntime().getRayConfig().numWorkersPerProcess;
  }

  @RayRemote
  public static class MyActor {

    @RayRemote
    public Integer getWorkersNum() {
      return TestUtils.getRuntime().getRayConfig().numWorkersPerProcess;
    }

    @RayRemote
    public String getJvmOptions() {
      return System.getProperty("X");
    }
  }

  @Test
  public void testJvmOptions() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<String> obj = Ray.call(JobConfigsTest::getJvmOptions);
    Assert.assertEquals("999", obj.get());
  }

  @Test
  public void testNumJavaWorkerPerProcess() {
    TestUtils.skipTestUnderSingleProcess();
    RayObject<Integer> obj = Ray.call(JobConfigsTest::getWorkersNum);
    Assert.assertEquals(3, (int) obj.get());
  }


  @Test
  public void testInActor() {
    TestUtils.skipTestUnderSingleProcess();
    RayActor<MyActor> actor = Ray.createActor(MyActor::new);

    // test jvm options.
    RayObject<String> obj1 = Ray.call(MyActor::getJvmOptions, actor);
    Assert.assertEquals("999", obj1.get());

    //  test workers number.
    RayObject<Integer> obj2 = Ray.call(MyActor::getWorkersNum, actor);
    Assert.assertEquals(3, (int) obj2.get());
  }
}
