package org.ray.streaming.io.channel;

/**
 * channel type
 * when run in single process, use memory channel, otherwise is pipe channel.
 */
public enum ChannelType {
  /**
   * support pipe channel/memory channel
   */
  PIPE(1),
  MEMORY(2);

  int code;

  ChannelType(int code) {
    this.code = code;
  }
}