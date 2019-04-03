package org.ray.streaming.core.runtime.collector;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.io.writer.RecordWriter;
import org.ray.streaming.message.Record;

public class ChannelCollector implements Collector<Record> {

  private RecordWriter recordWriter;

  @Override
  public void collect(Record value) {

  }
}