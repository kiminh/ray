package org.ray.streaming.io.writer;

import org.ray.streaming.io.channel.ChannelProducer;

/**
 * Record writer, upstream operator send record to downstream operators with a record writer.
 *
 * @param <T> the type of the record that can be emitted with this record writer
 */
public class RecordWriter<T> {
  private ChannelProducer channelConsumer;



  public void emit(T record) {
//    channelConsumer.send();
  }
}