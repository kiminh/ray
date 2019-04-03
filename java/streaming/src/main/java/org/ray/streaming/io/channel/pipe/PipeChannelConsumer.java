package org.ray.streaming.io.channel.pipe;

import java.util.List;
import java.util.Map;
import org.ray.streaming.io.channel.ChannelConsumer;
import org.ray.streaming.io.channel.ChannelRecords;
import org.ray.streaming.io.channel.OffsetAndMetadata;

public class PipeChannelConsumer implements ChannelConsumer {

  @Override
  public void subscribe(List<String> topics) {

  }

  @Override
  public void unsubscribe() {

  }

  @Override
  public void seek(Map<String, OffsetAndMetadata> topicInfo) {

  }

  @Override
  public ChannelRecords poll(long timeout) {
    return null;
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