package org.ray.streaming.io.channel.pipe;

import java.util.List;
import java.util.Map;
import org.ray.streaming.io.channel.ChannelProducer;
import org.ray.streaming.io.channel.ChannelRecord;
import org.ray.streaming.io.channel.OffsetAndMetadata;

public class PipeChannelProducer implements ChannelProducer {

  @Override
  public void createTopics(List topics) {

  }

  @Override
  public void close() {

  }

  @Override
  public void send(String topic, ChannelRecord record) {

  }

  @Override
  public Map<String, OffsetAndMetadata> position() {
    return null;
  }
}