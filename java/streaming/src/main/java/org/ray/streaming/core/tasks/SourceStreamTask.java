package org.ray.streaming.core.tasks;

import org.ray.streaming.core.processor.SourceProcessor;

public class SourceStreamTask<IN> extends StreamTask {
  private volatile boolean running = true;
  private SourceProcessor<IN> sourceProcessor;
  private Long batchId = 1L;

  public SourceStreamTask(SourceProcessor<IN> sourceProcessor) {
    this.sourceProcessor = sourceProcessor;
  }

  @Override
  protected void init() throws Exception {

  }

  @Override
  protected void cleanup() throws Exception {

  }

  @Override
  protected void cancelTask() throws Exception {

  }

  @Override
  public void run() {
    final SourceProcessor<IN> sourceProcessor = this.sourceProcessor;

    while (running) {
      sourceProcessor.process(batchId);
    }
  }
}