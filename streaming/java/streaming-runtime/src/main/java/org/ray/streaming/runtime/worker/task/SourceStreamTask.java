package org.ray.streaming.runtime.worker.task;

import org.ray.streaming.message.Record;
import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.core.processor.SourceProcessor;
import org.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class SourceStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;

  public SourceStreamTask(Processor sourceProcessor, JobWorker jobWorker) {
    super(sourceProcessor, jobWorker);
    this.sourceProcessor = (SourceProcessor) sourceProcessor;
  }

  @Override
  public void run() {
    LOG.info("Source stream task thread start.");
    try {
      while (jobWorker) {
        handelControMessage(jobWorker);
      }

      sourceProcessor.process(new Record(null));

      if (suspend) {
        continue;
      }
    } catch (Exception e) {

    }
  }
}
