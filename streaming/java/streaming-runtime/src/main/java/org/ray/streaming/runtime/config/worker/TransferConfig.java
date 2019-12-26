package org.ray.streaming.runtime.config.worker;

import org.ray.streaming.runtime.config.Config;

/**
 *
 */
public interface TransferConfig extends Config {

  String MEMORY_CHANNEL = "memory_channel";
  String NATIVE_CHANNEL = "native_channel";

  @DefaultValue(value = NATIVE_CHANNEL)
  @Key(value = "channel_type")
  String chennelType();

  @DefaultValue(value = "100000000")
  @Key(value = "channel_size")
  String channelSize();

  @DefaultValue(value = "false")
  @Key(value = "streaming.is_recreate")
  String isResreate();

  /**
   * @return return from DataReader.getBundle if only empty message read in this interval.
   */
  @DefaultValue(value = "-1")
  @Key(value = "timer_interval_ms")
  String timerIntervalMs();

  @DefaultValue(value = "")
  @Key(value = "streaming.ring_buffer_capacity")
  String streamingRingBufferCapacity();


  @DefaultValue(value = "")
  @Key(value = "streaming.empty_message_interval")
  String streamingEmptyMessageInterval();

  @DefaultValue(value = "1000")
  @Key(value = "streaming.queue.read.timeout.millis")
  long readMessageTimeOutMillis();
}
