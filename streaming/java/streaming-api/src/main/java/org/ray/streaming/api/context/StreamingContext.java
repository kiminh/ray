package org.ray.streaming.api.context;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;
import org.ray.streaming.api.stream.StreamSink;
import org.ray.streaming.jobgraph.JobGraph;
import org.ray.streaming.jobgraph.JobGraphBuilder;
import org.ray.streaming.schedule.IJobSchedule;

/**
 * Encapsulate the context information of a streaming Job.
 */
public class StreamingContext implements Serializable {

  private transient AtomicInteger idGenerator;
  /**
   * The sinks of this streaming job.
   */
  private List<StreamSink> streamSinks;
  private Map<String, Object> jobConfig;
  /**
   * The logic plan.
   */
  private JobGraph jobGraph;

  private StreamingContext(Map<String, Object> jobConfig) {
    this.idGenerator = new AtomicInteger(0);
    this.streamSinks = new ArrayList<>();
    this.jobConfig = jobConfig;
  }

  public static StreamingContext buildContext() {
    return new StreamingContext(new HashMap<>());
  }

  public static StreamingContext buildContext(Map<String, Object> jobConfig) {
    return new StreamingContext(jobConfig);
  }

  /**
   * Construct job DAG, and execute the job.
   */
  public void execute() {
    JobGraphBuilder jobGraphBuilder = new JobGraphBuilder(this.streamSinks, jobConfig);
    this.jobGraph = jobGraphBuilder.build();
    jobGraph.printPlan();

    ServiceLoader<IJobSchedule> serviceLoader = ServiceLoader.load(IJobSchedule.class);
    Iterator<IJobSchedule> iterator = serviceLoader.iterator();
    Preconditions.checkArgument(iterator.hasNext());
    // IJobSchedule jobSchedule = new JobScheduleImpl(jobConfig);
    IJobSchedule jobSchedule = iterator.next();
    jobSchedule.schedule(jobGraph, jobConfig);
  }

  public int generateId() {
    return this.idGenerator.incrementAndGet();
  }

  public void addSink(StreamSink streamSink) {
    streamSinks.add(streamSink);
  }

  public void withConfig(Map<String, Object> jobConfig) {
    this.jobConfig = jobConfig;
  }

  public Map<String, Object> getJobConfig() {
    return jobConfig;
  }
}
