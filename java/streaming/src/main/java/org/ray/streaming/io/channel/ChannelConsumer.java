package org.ray.streaming.io.channel;

import java.util.List;
import java.util.Map;

/**
 * Channel consumer
 */
public interface ChannelConsumer {

  void subscribe(List<String> topics);

  void unsubscribe();

  void seek(Map<String, OffsetAndMetadata> topicInfo);

  ChannelRecords poll(long timeout);

  Map<String, OffsetAndMetadata> position();

  void commit();

  void close();
}