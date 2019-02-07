package org.ray.api.test;

import com.google.common.collect.ImmutableList;
import java.util.concurrent.TimeUnit;
import org.ray.api.Ray;
import org.ray.api.RayObject;
import org.ray.api.exception.RayException;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayObjectImpl;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClientExceptionTest extends BaseTest {

  @Test
  public void testWaitAndCrash() {
    UniqueId randomId = UniqueId.randomId();
    RayObject<String> notExisting = new RayObjectImpl(randomId);

    Thread thread = new Thread(() -> {
      try {
        TimeUnit.SECONDS.sleep(1);
        Ray.shutdown();
      } catch (InterruptedException e) {
        System.out.println("Got InterruptedException when sleeping, exit right now.");
        throw new RuntimeException("Got InterruptedException when sleeping.", e);
      }
    });
    thread.start();
    try {
      Ray.wait(ImmutableList.of(notExisting), 1, 2000);
      Assert.fail("Should not reach here");
    } catch (RayException e) {
      System.out.println(String.format("Expected runtime exception: {}", e));
    }
    try {
      thread.join();
    } catch (Exception e) {
      System.out.println(e);
    }
  }
}
