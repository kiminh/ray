package org.ray.streaming.runtime.core.state.serde;

public interface IKVStoreSerDe<K, V> extends IKStoreSerDe<K> {

  byte[] serValue(V value);

  V deSerValue(byte[] valueArray);
}
