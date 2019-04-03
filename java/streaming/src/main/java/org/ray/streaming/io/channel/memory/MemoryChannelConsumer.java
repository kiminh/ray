package org.ray.streaming.io.channel.memory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.ray.streaming.io.channel.ChannelConsumer;
import org.ray.streaming.io.channel.ChannelRecord;
import org.ray.streaming.io.channel.ChannelRecords;
import org.ray.streaming.io.channel.OffsetAndMetadata;

public class MemoryChannelConsumer implements ChannelConsumer {

  private List<String> topics = new ArrayList<>();
  public static Map<String, Queue<Object>> topicQueues = new ConcurrentHashMap<>();

  @Override
  public void subscribe(List<String> topics) {
    this.topics.addAll(topics);
  }

  @Override
  public void unsubscribe() {
    topics.clear();
  }

  @Override
  public void seek(Map<String, OffsetAndMetadata> topicInfo) {
  }

  @Override
  public ChannelRecords poll(long timeout) {
    int index = 0;
    for (; index < topics.size(); ++index) {
      String topic = topics.get(index);
      Queue<Object> queue = topicQueues.get(topic);
      if (null == queue || queue.isEmpty()) {
        continue;
      }

      break;
    }

    ChannelRecords channelRecords = new ChannelRecords();
    if (index < topics.size()) {
      String qid = topics.get(index);
      Queue<Object> queue = topicQueues.get(qid);
      ChannelRecord record = (ChannelRecord) queue.poll();
      channelRecords.add(record);
    }

    return channelRecords;
  }

  @Override
  public Map<String, OffsetAndMetadata> position() {
    return null;
  }

  @Override
  public void commit() {

  }

  @Override
  public void close() {

  }
}