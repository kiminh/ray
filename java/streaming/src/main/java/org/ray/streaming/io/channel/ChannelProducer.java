package org.ray.streaming.io.channel;

import java.util.List;
import java.util.Map;

/**
 * Channel producer
 */
public interface ChannelProducer<T> {

  void createTopics(List<String> topics);

  void close();

  void send(String topic, ChannelRecord<T> record);

  Map<String, OffsetAndMetadata> position();
}
