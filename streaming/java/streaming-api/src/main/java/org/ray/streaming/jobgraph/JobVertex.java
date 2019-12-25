package org.ray.streaming.jobgraph;

import java.io.Serializable;

import com.google.common.base.MoreObjects;
import org.ray.streaming.operator.StreamOperator;

/**
 * JobVertex is a cell node where logic is executed.
 */
public class JobVertex implements Serializable {

  private final int vertexId;
  private final int parallelism;
  private final String vertexName;
  private final VertexType vertexType;
  private final LanguageType languageType;
  private final StreamOperator streamOperator;

  public JobVertex(int vertexId, int parallelism, VertexType vertexType,
      StreamOperator streamOperator) {
    this(vertexId, parallelism, vertexType, LanguageType.JAVA, streamOperator);
  }

  public JobVertex(int vertexId, int parallelism, VertexType vertexType, LanguageType languageType,
      StreamOperator streamOperator) {
    this.vertexId = vertexId;
    this.parallelism = parallelism;
    this.vertexType = vertexType;
    this.languageType = languageType;
    this.streamOperator = streamOperator;
    this.vertexName = generateVertexName(streamOperator);
}

  private String generateVertexName(StreamOperator streamOperator) {
    return vertexId + "-" + streamOperator.getName();
  }

  public int getVertexId() {
    return vertexId;
  }

  public int getParallelism() {
    return parallelism;
  }

  public String getVertexName() {
    return vertexName;
  }

  public VertexType getVertexType() {
    return vertexType;
  }

  public LanguageType getLanguageType() {
    return languageType;
  }

  public StreamOperator getStreamOperator() {
    return streamOperator;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("vertexId", vertexId)
        .add("parallelism", parallelism)
        .add("vertexName", vertexName)
        .add("vertexType", vertexType)
        .add("languageType", languageType)
        .add("streamOperator", streamOperator)
        .toString();
  }
}
