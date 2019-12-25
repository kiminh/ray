package org.ray.streaming.runtime.driver;

import java.util.Map;

import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.driver.IJobDriver;
import org.slf4j.Logger;

import org.ray.streaming.runtime.util.LoggerFactory;

/**
 * Job driver: used to submit job to runtime.
 */
public class JobDriverImpl implements IJobDriver {

  public static final Logger LOG = LoggerFactory.getLogger(JobDriverImpl.class);

  @Override
  public void submit(JobGraph jobGraph, Map<String, String> conf) {
    LOG.info("Submit job [{}] with job graph [{}] and job config [{}].",
        jobGraph.getJobName(), jobGraph, conf);
  }
}
