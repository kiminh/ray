package org.ray.streaming.runtime.core.state.serde;

import java.io.Serializable;

public interface IKStoreSerDe<K> extends Serializable {
    byte[] serKey(K key);
}
