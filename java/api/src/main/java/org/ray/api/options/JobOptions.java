package org.ray.api.options;

import java.util.Map;

public class JobOptions {

  public static JobOptions DEFAULT = new JobOptions.Builder().createJobOptions();

  public final CallOptions callOptions;

  public final boolean finishJobWhenRootTaskFinishes;

  private JobOptions(CallOptions callOptions, boolean finishJobWhenRootTaskFinishes) {
    this.callOptions = callOptions;
    this.finishJobWhenRootTaskFinishes = finishJobWhenRootTaskFinishes;
  }

  public static class Builder {

    private CallOptions.Builder callOptionsBuilder = new CallOptions.Builder();
    private boolean finishJobWhenRootTaskFinishes = true;

    public Builder setResourcesForRootTask(Map<String, Double> resources) {
      callOptionsBuilder.setResources(resources);
      return this;
    }

    public Builder setFinishJobWhenRootTaskFinishes(boolean finishJobWhenRootTaskFinishes) {
      this.finishJobWhenRootTaskFinishes = finishJobWhenRootTaskFinishes;
      return this;
    }

    public JobOptions createJobOptions() {
      return new JobOptions(callOptionsBuilder.createCallOptions(), finishJobWhenRootTaskFinishes);
    }
  }
}
