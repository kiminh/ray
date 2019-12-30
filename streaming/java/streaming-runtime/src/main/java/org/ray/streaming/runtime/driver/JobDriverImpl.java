package org.ray.streaming.runtime.driver;

import java.util.HashMap;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.options.ActorCreationOptions;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.driver.IJobDriver;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.global.CommonConfig;
import org.ray.streaming.runtime.master.JobMaster;
import org.ray.streaming.runtime.util.KryoUtils;
import org.ray.streaming.runtime.util.LoggerFactory;

/**
 * Job driver: to submit job from api to runtime.
 */
public class JobDriverImpl implements IJobDriver {

  public static final Logger LOG = LoggerFactory.getLogger(JobDriverImpl.class);

  private RayActor<JobMaster> jobMasterActor;

  @Override
  public void submit(JobGraph jobGraph, Map<String, String> jobConfig) {
    LOG.info("Submit job [{}] with job graph [{}] and job config [{}].",
        jobGraph.getJobName(), jobGraph, jobConfig);
    Map<String, Double> resources = new HashMap<>();
    ActorCreationOptions options = new ActorCreationOptions.Builder()
        .setResources(resources)
        .setMaxReconstructions(ActorCreationOptions.INFINITE_RECONSTRUCTIONS)
        .createActorCreationOptions();

    // set job name and id at start
    jobConfig.put(CommonConfig.JOB_ID, Ray.getRuntimeContext().getCurrentJobId().toString());
    jobConfig.put(CommonConfig.JOB_NAME, jobGraph.getJobName());

    this.jobMasterActor = Ray.createActor(JobMaster::new, jobConfig, options);
    RayObject<Boolean> submitResult = Ray.call(JobMaster::submitJob, jobMasterActor, jobMasterActor,
        KryoUtils.writeToByteArray(jobGraph));

    Ray.get(submitResult.getId());
    if (submitResult.get()) {
      LOG.info("Submit job [{}] success.", jobGraph.getJobName());
    }
  }
}
