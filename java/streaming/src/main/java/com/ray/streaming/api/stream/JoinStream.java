package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.StreamingContext;
import com.ray.streaming.api.function.impl.JoinFunction;
import com.ray.streaming.api.function.impl.KeyFunction;
import com.ray.streaming.operator.StreamOperator;
import java.io.Serializable;

/**
 * Represents an DataStream of Two DataStream Joined.
 * @param <T> The type of left Join Stream Data.
 * @param <O> The type of right join Stream Data.
 * @param <R> The type of result joined Stream Data.
 */
public class JoinStream<T, O, R> extends DataStream<T> {

  public JoinStream(StreamingContext streamingContext, StreamOperator streamOperator) {
    super(streamingContext, streamOperator);
  }

  public JoinStream(DataStream<T> leftStream, DataStream<O> rightStream) {
    super(leftStream, null);
  }

  /**
   * Apply key-by to left join stream.
   */
  public <K> Where<T, O, R, K> where(KeyFunction<T, K> keyFunction) {
    return new Where<>(this, keyFunction);
  }

  /**
   * Where clause of Join Transformation.
   * @param <T> The type of left Join Stream Data.
   * @param <O> The type of right Join Stream Data.
   * @param <R> The type of result joined Stream Data.
   * @param <K> The type of Join Key.
   */
  class Where<T, O, R, K> implements Serializable {

    private JoinStream<T, O, R> joinStream;
    private KeyFunction<T, K> leftKeyByFunction;

    public Where(JoinStream<T, O, R> joinStream, KeyFunction<T, K> leftKeyByFunction) {
      this.joinStream = joinStream;
      this.leftKeyByFunction = leftKeyByFunction;
    }

    public Equal<T, O, R, K> equalTo(KeyFunction<O, K> rightKeyFunction) {
      return new Equal<>(joinStream, leftKeyByFunction, rightKeyFunction);
    }
  }

  /**
   * Equal clause of Join Transformation.
   * @param <T> The type of left Join Stream Data.
   * @param <O> The type of right Join Stream Data.
   * @param <R> The type of result joined Stream Data.
   * @param <K> The type of Join Key.
   */
  class Equal<T, O, R, K> implements Serializable {

    private JoinStream<T, O, R> joinStream;
    private KeyFunction<T, K> leftKeyByFunction;
    private KeyFunction<O, K> rightKeyByFunction;

    public Equal(JoinStream<T, O, R> joinStream, KeyFunction<T, K> leftKeyByFunction,
        KeyFunction<O, K> rightKeyByFunction) {
      this.joinStream = joinStream;
      this.leftKeyByFunction = leftKeyByFunction;
      this.rightKeyByFunction = rightKeyByFunction;
    }

    public DataStream<R> with(JoinFunction<T, O, R> joinFunction) {
      return (DataStream<R>) joinStream;
    }
  }

}
