package org.ray.streaming.io.channel;

import java.util.List;
import java.util.Map;

/**
 * Channel producer
 */
public interface ChannelProducer<T> {

  void createTopic(List<String> topic);

  void close();

  void send(ChannelRecord<T> record);

  Map<String, OffsetAndMetadata> position();
}
