package org.ray.streaming.io.channel.memory;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.ray.streaming.io.channel.ChannelProducer;
import org.ray.streaming.io.channel.ChannelRecord;
import org.ray.streaming.io.channel.OffsetAndMetadata;

public class MemoryChannelProducer<T> implements ChannelProducer<T> {
  private final static int QUEUE_SIZE_MAX = 100;

  private Map<String, Queue<Object>> topicQueues;

  public MemoryChannelProducer() {
    this.topicQueues = MemoryChannelConsumer.topicQueues;
  }

  @Override
  public void createTopics(List<String> topics) {
    topics.forEach(topic -> {
      Queue<Object> queue = topicQueues.get(topic);
      if (null == queue) {
        queue = new ConcurrentLinkedQueue<>();
        topicQueues.put(topic, queue);
      }
    });
  }

  @Override
  public void close() {

  }

  @Override
  public void send(String topic, ChannelRecord record) {
    Queue<Object> queue = topicQueues.get(topic);
    if (null == queue) {
      queue = new ConcurrentLinkedQueue<>();
      topicQueues.put(topic, queue);
    }

    // check if back pressure
    while (queue.size() > QUEUE_SIZE_MAX) {
      try {
        Thread.sleep(10);
      } catch (Exception e) {
        // do nothing
      }
    }

    queue.add(record);
  }

  @Override
  public Map<String, OffsetAndMetadata> position() {
    return null;
  }
}