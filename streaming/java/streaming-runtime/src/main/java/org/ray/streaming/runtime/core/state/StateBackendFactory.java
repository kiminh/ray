package org.ray.streaming.runtime.core.state;

import org.ray.streaming.runtime.config.StreamingGlobalConfig;
import org.ray.streaming.runtime.config.types.StateBackendType;
import org.ray.streaming.runtime.core.state.impl.MemoryStateBackend;

public class StateBackendFactory {

  public static StateBackend getStateBackend(final StreamingGlobalConfig config) {
    StateBackend stateBackend = null;
    StateBackendType type = StateBackendType.valueOf(
        config.stateBackendConfig.stateBackendType().toUpperCase());

    switch (type) {
      case MEMORY:
        stateBackend = new MemoryStateBackend(config.stateBackendConfig);
        break;
      default:
        stateBackend = new MemoryStateBackend(config.stateBackendConfig);
        break;
    }
    return stateBackend;
  }
}
