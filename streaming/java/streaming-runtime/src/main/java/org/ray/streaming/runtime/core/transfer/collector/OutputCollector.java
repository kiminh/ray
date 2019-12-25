package org.ray.streaming.runtime.core.transfer.collector;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.ray.runtime.util.Serializer;
import org.ray.streaming.message.Record;
import org.slf4j.Logger;

import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.runtime.core.transfer.ChannelID;
import org.ray.streaming.runtime.core.transfer.DataWriter;
import org.ray.streaming.runtime.util.LoggerFactory;

public class OutputCollector implements Collector<Record> {

  private static final Logger LOG = LoggerFactory.getLogger(OutputCollector.class);

  private Partition partition;
  private DataWriter writer;
  private ChannelID[] outputQueues;

  public OutputCollector(Collection<String> outputQueueIds, DataWriter writer, Partition partition) {
    this.outputQueues = outputQueueIds.stream().map(ChannelID::from).toArray(ChannelID[]::new);
    this.writer = writer;
    this.partition = partition;
    LOG.debug("OutputCollector constructed, outputQueueIds:{}, partition:{}.", outputQueueIds, this.partition);
  }

  @Override
  public void collect(Record record) {
    int[] partitions = this.partition.partition(record, outputQueues.length);
    ByteBuffer msgBuffer = ByteBuffer.wrap(Serializer.encode(record));
    for (int partition : partitions) {
      writer.write(outputQueues[partition], msgBuffer);
    }
  }

}
