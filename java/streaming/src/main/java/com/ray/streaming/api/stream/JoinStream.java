package com.ray.streaming.api.stream;

import com.ray.streaming.api.context.RayContext;
import com.ray.streaming.api.function.impl.JoinFunction;
import com.ray.streaming.api.function.impl.KeyFunction;
import com.ray.streaming.operator.StreamOperator;
import java.io.Serializable;

/**
 * Join DataStream.
 * @param <T> left stream element type
 * @param <O> right stream element type
 * @param <R> result stream element type
 */
public class JoinStream<T, O, R> extends DataStream<T> {

  public JoinStream(RayContext rayContext, StreamOperator streamOperator) {
    super(rayContext, streamOperator);
  }

  public JoinStream(DataStream<T> leftStream, DataStream<O> rightStream) {
    super(leftStream, null);
  }

  public <K> Where<T, O, R, K> where(KeyFunction<T, K> keyFunction) {
    return new Where<>(this, keyFunction);
  }

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
