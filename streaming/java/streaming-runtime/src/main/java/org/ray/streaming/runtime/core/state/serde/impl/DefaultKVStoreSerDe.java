package org.ray.streaming.runtime.core.state.serde.impl;

import com.alipay.streaming.runtime.state.serde.IKVStoreSerDe;
import com.alipay.streaming.runtime.utils.KryoUtils;
import org.apache.hadoop.hbase.util.Bytes;

public class DefaultKVStoreSerDe<K, V> extends AbstractSerDe implements IKVStoreSerDe<K, V> {

  public DefaultKVStoreSerDe(String keyPrefix) {
    super(keyPrefix);
  }

  /**
   * Generate row key with prefix
   * @param key String
   * @return byte[]
   */
  @Override
  public byte[] serKey(K key) {
    String keyWithPrefix = generateRowKeyWithPrefix(key.toString());
    return Bytes.toBytes(keyWithPrefix);
  }

  /**
   * Serialize value to byte[]
   * @param value
   * @return byte[]
   */
  @Override
  public byte[] serValue(V value) {
    return KryoUtils.writeToByteArray(value);
  }

  /**
   * Deserialize byte[] value to object
   * @param valueArray
   * @return
   */
  @Override
  public V deSerValue(byte[] valueArray) {
    return (V) KryoUtils.readFromByteArray(valueArray);
  }
}
