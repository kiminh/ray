package org.ray.streaming.io.reader;

import org.ray.streaming.io.channel.ChannelConsumer;

/**
 * Record reader, downstream operator read record from upstream operators with a record reader.
 *
 * @param <T> Thy type of the records that is read.
 */
public class RecordReader<T> {
  private ChannelConsumer channelConsumer;
  private T currentRecord;

  public boolean hasNext() {
    return false;
  }

  public T next() {
    return currentRecord;
  }
}