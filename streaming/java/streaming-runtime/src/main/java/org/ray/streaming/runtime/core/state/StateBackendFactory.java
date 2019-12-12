package org.ray.streaming.runtime.core.state;

import com.alipay.streaming.runtime.config.StreamingGlobalConfig;
import com.alipay.streaming.runtime.config.types.StateBackendType;
import com.alipay.streaming.runtime.state.impl.AtomicFsBackend;
import com.alipay.streaming.runtime.state.impl.MemoryStateBackend;
import com.alipay.streaming.runtime.state.impl.hbase.HBaseStateBackend;

public class StateBackendFactory {

  public static StateBackend getStateBackend(final StreamingGlobalConfig config) {
    StateBackend stateBackend = null;
    StateBackendType type = StateBackendType.valueOf(
        config.stateBackendConfig.stateBackendType().toUpperCase());

    switch (type) {
      case MEMORY:
        stateBackend = new MemoryStateBackend(config.stateBackendConfig);
        break;
      case PANGU:
        stateBackend = new AtomicFsBackend(config.stateBackendPanguConfig);
        break;
      case HBASE:
        stateBackend = new HBaseStateBackend(config.stateBackendHBaseConfig);
        break;
      default:
        break;
    }
    return stateBackend;
  }
}
