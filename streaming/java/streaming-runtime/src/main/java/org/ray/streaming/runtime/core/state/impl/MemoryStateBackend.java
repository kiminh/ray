package org.ray.streaming.runtime.core.state.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.global.StateBackendConfig;
import org.ray.streaming.runtime.core.state.StateBackend;
import org.ray.streaming.runtime.util.LoggerFactory;

public class MemoryStateBackend implements StateBackend<String, byte[], StateBackendConfig> {

  private static final Logger LOG = LoggerFactory.getLogger(MemoryStateBackend.class);

  private final Map<String, byte[]> kvStore = new HashMap<>();

  public MemoryStateBackend(StateBackendConfig config) {
    init(config);
  }

  @Override
  public void init(final StateBackendConfig config) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Start init memory state backend, config is {}.", config);
      LOG.info("Finish init memory state backend.");
    }
  }

  @Override
  public boolean exists(String key) {
    return kvStore.containsKey(key);
  }

  @Override
  public byte[] get(final String key) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Get value of key {} start.", key);
    }

    byte[] readData = kvStore.get(key);

    if (LOG.isInfoEnabled()) {
      LOG.info("Get value of key {} success.", key);
    }

    return readData;
  }

  @Override
  public List<byte[]> batchGet(final List<String> keys) {
    throw new UnsupportedOperationException("batchGet not support.");
  }

  @Override
  public void put(final String key, final byte[] value) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Put value of key {} start.", key);
    }

    kvStore.put(key, value);

    if (LOG.isInfoEnabled()) {
      LOG.info("Put value of key {} success.", key);
    }
  }

  @Override
  public void batchPut(Map<String, byte[]> batchData) {
    throw new NotImplementedException("batchPut is not implemented!");
  }

  @Override
  public void remove(final String key) {
    if (LOG.isInfoEnabled()) {
      LOG.info("Remove value of key {} start.", key);
    }

    kvStore.remove(key);

    if (LOG.isInfoEnabled()) {
      LOG.info("Remove value of key {} success.", key);
    }
  }

  @Override
  public void flush() {

  }
}