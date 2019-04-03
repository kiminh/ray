package org.ray.streaming.core.tasks;

import org.ray.streaming.core.processor.OneInputProcessor;

public class OneInputStreamTask<IN> extends StreamTask {
  private volatile boolean running = true;
  private OneInputProcessor<IN> inputProcessor;

  public OneInputStreamTask(OneInputProcessor<IN> inputProcessor) {
    this.inputProcessor = inputProcessor;
  }

  @Override
  protected void init() throws Exception {

  }

  @Override
  public void run() {
    final OneInputProcessor<IN> inputProcessor = this.inputProcessor;

    while (running && inputProcessor.processInput()) {
      // do nothing
    }
  }

  @Override
  protected void cleanup() throws Exception {

  }

  @Override
  protected void cancelTask() throws Exception {

  }
}