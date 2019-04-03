package org.ray.streaming.io.writer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.ray.api.RayActor;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.core.graph.ExecutionEdge;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.core.runtime.StreamWorker;
import org.ray.streaming.io.channel.ChannelFactory;
import org.ray.streaming.io.channel.ChannelProducer;
import org.ray.streaming.io.channel.ChannelRecord;
import org.ray.streaming.io.channel.ChannelType;
import org.ray.streaming.message.Record;

/**
 * Record writer, upstream operator send record to downstream operators with a record writer.
 */
public class RecordWriter implements Collector<Record>  {
  private int taskId;
  private int[] targetTaskIds;
  private Partition partition;
  private ChannelProducer channelProducer;

  public RecordWriter(int taskId, ExecutionEdge executionEdge, ExecutionGraph executionGraph) {
    this.taskId = taskId;
    int targetNodeId = executionEdge.getTargetNodeId();
    Map<Integer, RayActor<StreamWorker>> taskId2Worker = executionGraph
        .getTaskId2WorkerByNodeId(targetNodeId);
    this.targetTaskIds = Arrays.stream(taskId2Worker.keySet()
        .toArray(new Integer[taskId2Worker.size()]))
        .mapToInt(Integer::valueOf).toArray();

    this.partition = executionEdge.getPartition();
    this.channelProducer = ChannelFactory.getProducer(ChannelType.MEMORY);
    List<String> topics = new ArrayList<>();
    for (int targetTaskId : targetTaskIds) {
      topics.add(genTopicByTaskId(taskId, targetTaskId));
    }
    this.channelProducer.createTopics(topics);
  }

  @Override
  public void collect(Record record) {
    int[] taskIds = this.partition.partition(record, targetTaskIds);

    for (int targetTaskId : taskIds) {
      channelProducer.send(genTopicByTaskId(taskId, targetTaskId), new ChannelRecord(record));;
    }
  }

  private String genTopicByTaskId(int srcTaskId, int targetTaskId) {
    return srcTaskId + "->" + targetTaskId;
  }
}