package org.ray.streaming.io.channel;

import org.ray.streaming.io.channel.memory.MemoryChannelConsumer;
import org.ray.streaming.io.channel.memory.MemoryChannelProducer;
import org.ray.streaming.io.channel.pipe.PipeChannelConsumer;
import org.ray.streaming.io.channel.pipe.PipeChannelProducer;

public class ChannelFactory {

  public static ChannelConsumer getConsumer(ChannelType type) {
    ChannelConsumer consumer = null;
    switch (type) {
      case PIPE:
        consumer = new PipeChannelConsumer();
        break;
      case MEMORY:
        consumer = new MemoryChannelConsumer();
        break;
      default:
        break;
    }
    return consumer;
  }

  public static ChannelProducer getProducer(ChannelType type) {
    ChannelProducer producer = null;
    switch (type) {
      case PIPE:
        producer = new PipeChannelProducer();
        break;
      case MEMORY:
        producer = new MemoryChannelProducer();
        break;
      default:
        break;
    }
    return producer;
  }

}