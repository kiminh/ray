package org.ray.streaming.runtime.core.state.impl;

import com.alipay.streaming.runtime.config.global.StateBackendPanguConfig;
import com.alipay.streaming.runtime.config.types.FsBackendCreationType;
import com.alipay.streaming.runtime.utils.LoggerFactory;
import java.util.List;
import org.slf4j.Logger;

/**
 * achieves an atomic `put` method.
 * known issue: if you crashed while write a key at first time, this code will not work.
 */
public class AtomicFsBackend extends FsStateBackend {

  private static final Logger LOG = LoggerFactory.getLogger(AtomicFsBackend.class);
  private static final String TMP_FLAG = "_tmp";

  public AtomicFsBackend(final StateBackendPanguConfig config) {
    super(config);
  }

  public AtomicFsBackend(final StateBackendPanguConfig config,
      FsBackendCreationType fsBackendCreationType) {
    super(config, fsBackendCreationType);
  }

  @Override
  public List<byte[]> batchGet(final List<String> keys) {
    throw new UnsupportedOperationException("batchGet not support.");
  }

  @Override
  public byte[] get(String key) throws Exception {
    String tmpKey = key + TMP_FLAG;
    if (super.exists(tmpKey) && !super.exists(key)) {
      return super.get(tmpKey);
    }
    return super.get(key);
  }

  @Override
  public void put(String key, byte[] value) throws Exception {
    String tmpKey = key + TMP_FLAG;
    if (super.exists(tmpKey) && !super.exists(key)) {
      super.rename(tmpKey, key);
    }
    super.put(tmpKey, value);
    super.remove(key);
    super.rename(tmpKey, key);
  }

  @Override
  public void remove(String key) throws Exception {
    String tmpKey = key + TMP_FLAG;
    if (super.exists(tmpKey)) {
      super.remove(tmpKey);
    }
    super.remove(key);
  }
}
