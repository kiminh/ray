package org.ray.api.test;

import org.ray.api.job.RayJob;
import org.ray.api.job.RayJobs;
import org.ray.api.options.JobOptions;
import org.testng.annotations.Test;

public class SubJobTest extends BaseTest {

  public static String jobRootTask(String str) {
    return str;
  }

  public static void jobRootTaskNoReturn() {
  }

  @Test
  public void testSubmitJob() {
    RayJob<String> job1 = RayJobs.submitJob(SubJobTest::jobRootTask, "foo", JobOptions.DEFAULT);
    RayJob<Void> job2 = RayJobs.submitJob(SubJobTest::jobRootTaskNoReturn, JobOptions.DEFAULT);
  }
}
