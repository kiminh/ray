package org.ray.streaming.runtime.worker.task;

import org.ray.streaming.runtime.core.processor.Processor;
import org.ray.streaming.runtime.transfer.Message;
import org.ray.streaming.runtime.transfer.QueueMessage;
import org.ray.streaming.runtime.util.Serializer;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 *
 */
public abstract class InputStreamTask extends StreamTask {

  private long readTimeOutMillis;
  public InputStreamTask(int taskId, Processor processor, JobWorker jobWorker) {
    super(taskId, processor, jobWorker);
    readTimeOutMillis = jobWorker.getWorkerConfig().transferConfig.readMessageTimeOutMillis();
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    while (running) {
      Message message = reader.read(readTimeOutMillis);
      if (message != null) {
        byte[] bytes = new byte[message.body().remaining()];
        Object obj = Serializer.decode(bytes);
        if (obj instanceof QueueMessage) {
          processor.process(obj);
        } else {
          throw new IllegalArgumentException("Unsupported queue item type:" + obj);
        }
      }
    }
  }

  @Override
  protected void cancelTask() throws Exception {
    running = false;
    while (!stopped) {
    }
  }
}
