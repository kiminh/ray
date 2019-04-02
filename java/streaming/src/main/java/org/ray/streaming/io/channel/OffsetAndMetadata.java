package org.ray.streaming.io.channel;

public class OffsetAndMetadata {
  private final String metadata;
  private final long offset;

  public OffsetAndMetadata(String metadata, long offset) {
    this.metadata = metadata;
    this.offset = offset;
  }

  public String metadata() {
    return metadata;
  }

  public long offset() {
    return offset;
  }
}