package com.ray.streaming.core.runtime.context;


public interface RuntimeContext {

  int getTaskId();

  int getTaskIndex();

  int getParallelism();

  Long getBatchId();
}
