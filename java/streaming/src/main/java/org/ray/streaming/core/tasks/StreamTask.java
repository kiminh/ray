package org.ray.streaming.core.tasks;

public abstract class StreamTask implements Runnable {
  protected abstract void init() throws Exception;

  protected abstract void cleanup() throws Exception;

  protected abstract void cancelTask() throws Exception;
}