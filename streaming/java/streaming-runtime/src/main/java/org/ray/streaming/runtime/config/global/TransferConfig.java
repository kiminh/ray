package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;
import org.ray.streaming.runtime.config.types.TransferChannelType;

/**
 * Job data transfer config.
 */

public interface TransferConfig extends Config {

  String CHANNEL_TYPE = "streaming.transfer.channel.type";
  String CHANNEL_SIZE = "streaming.transfer.channel.size";
  String READER_IS_RECREATE = "streaming.transfer.channel.is-recreate";
  String READER_TIMER_INTERVAL_MS = "streaming.transfer.channel.timer.interval.ms";
  String RING_BUFFER_CAPACITY = "streaming.transfer.ring-buffer.capacity";
  String EMPTY_MSG_INTERVAL = "streaming.transfer.empty-message.interval";

  @DefaultValue(value = "NATIVE_CHANNEL")
  @Key(value = CHANNEL_TYPE)
  TransferChannelType channelType();

  @DefaultValue(value = "100000000")
  @Key(value = CHANNEL_SIZE)
  long channelSize();

  @DefaultValue(value = "false")
  @Key(value = READER_IS_RECREATE)
  boolean readerIsRecreate();

  @DefaultValue(value = "-1")
  @Key(value = READER_TIMER_INTERVAL_MS)
  long readerTimerIntervalMs();

  @DefaultValue(value = "-1")
  @Key(value = RING_BUFFER_CAPACITY)
  int ringBufferCapacity();

  @DefaultValue(value = "-1")
  @Key(value = EMPTY_MSG_INTERVAL)
  int emptyMsgInterval();
}
