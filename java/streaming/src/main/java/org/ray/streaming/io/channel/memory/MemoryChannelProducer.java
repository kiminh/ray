package org.ray.streaming.io.channel.memory;

import java.util.List;
import java.util.Map;

import org.ray.streaming.io.channel.ChannelProducer;
import org.ray.streaming.io.channel.ChannelRecord;
import org.ray.streaming.io.channel.OffsetAndMetadata;

public class MemoryChannelProducer implements ChannelProducer {

  @Override
  public void createTopic(List topic) {

  }

  @Override
  public void close() {

  }

  @Override
  public void send(ChannelRecord record) {

  }

  @Override
  public Map<String, OffsetAndMetadata> position() {
    return null;
  }
}