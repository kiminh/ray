package org.ray.streaming.runtime.worker.task;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public abstract class StreamTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected Processor processor;
  protected JobWorker jobWorker;

  protected volatile boolean suspend = false;

  private QueueProducer producer;
  private QueueConsumer consumer;

  protected StreamTask(Processor processor, JobWorker jobWorker) {
    this.processor = processor;
    this.jobWorker = jobWorker;

    this.producer = new QueueProducer();
    this.consumer = new QueueConsumer();
  }

  protected void handelControMessage(ControlMessage message) {
    switch (message.getMessageType()) {
      case SUSPEND:
        suspend();
        break;
      case RESUME:
        resume();
        break;
      default:
          throw new IllegalStateException("Unsupported message: " + message);
    }
  }

  private void suspend() {
    LOG.info("Suspend stream task thread.");
    this.suspend = true;
  }

  private void resume() {
    LOG.info("Resume stream task thread.");
    this.suspend = false;
  }


  public void close() throws Exception {
    close0(true);
  }

  private void close0(boolean forceExit) throws Exception {
    LOG.info("Stream task begin close.");

    if (producer != null) {
      producer.stop();
    }

    if (consumer != null) {
      consumer.stop();
    }

  }
}
