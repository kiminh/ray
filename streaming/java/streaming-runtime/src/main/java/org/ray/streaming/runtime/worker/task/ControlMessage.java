package org.ray.streaming.runtime.worker.task;

import java.io.Serializable;

/**
 *
 */
public class ControlMessage<T> implements Serializable {

  private final T message;
  private final MessageType messageType;
  private final Object extra;

  public ControlMessage(T message, MessageType messageType, Object extra) {
    this.message = message;
    this.messageType = messageType;
    this.extra = extra;
  }

  public MessageType getMessageType() {
    return messageType;
  }

  public enum MessageType {
    SUSPEND(1, "suspend source"),
    RESUME(2, "resume source");

    private int index;
    private String value;

    MessageType(int index, String value) {
      this.index = index;
      this.value = value;
    }
  }

  @Override
  public String toString() {
    return "ControlMessage{" +
        "message=" + message +
        ", messageType=" + messageType +
        ", extra=" + extra +
        '}';
  }
}
