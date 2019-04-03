package org.ray.streaming.io.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.ray.api.RayActor;
import org.ray.streaming.core.graph.ExecutionEdge;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.io.channel.ChannelConsumer;
import org.ray.streaming.io.channel.ChannelFactory;
import org.ray.streaming.io.channel.ChannelRecord;
import org.ray.streaming.io.channel.ChannelRecords;
import org.ray.streaming.io.channel.ChannelType;

/**
 * Record reader, downstream operator read record from upstream operators with a record reader.
 *
 * @param <T> Thy type of the records that is read.
 */
public class RecordReader<T> {
  private ChannelConsumer channelConsumer;
  private T currentRecord;
  private List<ChannelRecord<T>> currentRecords = new ArrayList<>();

  public RecordReader(int taskId, ExecutionEdge executionEdge, ExecutionGraph executionGraph) {
    int srcNodeId = executionEdge.getSrcNodeId();
    Map<Integer, RayActor<StreamWorker>> taskId2Worker = executionGraph
        .getTaskId2WorkerByNodeId(srcNodeId);
    Set<Integer> srcTaskIds = taskId2Worker.keySet();

    List<String> topics = new ArrayList<>();
    for (Integer srcTaskId : srcTaskIds) {
      topics.add(genTopicByTaskId(srcTaskId, taskId));
    }
    this.channelConsumer = ChannelFactory.getConsumer(ChannelType.MEMORY);
    this.channelConsumer.subscribe(topics);
  }

  public boolean hasNext() {
    currentRecord = null;
    if (currentRecords.isEmpty()) {
      ChannelRecords channelRecords = channelConsumer.poll(0);
      currentRecords = channelRecords.records();
    }

    if (!currentRecords.isEmpty()) {
      currentRecord = currentRecords.remove(0).value();
    }

    return currentRecord != null;
  }

  public T next() {
    return currentRecord;
  }

  private String genTopicByTaskId(int srcTaskId, int targetTaskId) {
    return srcTaskId + "->" + targetTaskId;
  }
}