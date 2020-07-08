package io.ray.api.test;

import io.ray.api.Ray;
import io.ray.api.ObjectRef;
import io.ray.api.ActorHandle;
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
    ObjectRef<Integer> obj = Ray.task(JobConfigsTest::getWorkersNum).remote();
    Assert.assertEquals(3, (int) obj.get());
  }


  @Test
  public void testInActor() {
    TestUtils.skipTestUnderSingleProcess();
    ActorHandle<MyActor> actor = Ray.actor(MyActor::new).remote();

    // test jvm options.
    RayObject<String> obj1 = Ray.call(MyActor::getJvmOptions, actor);
    Assert.assertEquals("999", obj1.get());

    //  test workers number.
    ObjectRef<Integer> obj2 = Ray.task(MyActor::getWorkersNum, actor).remote();
    Assert.assertEquals(3, (int) obj2.get());
  }
}
