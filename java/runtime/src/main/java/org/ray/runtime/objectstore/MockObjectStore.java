package org.ray.runtime.objectstore;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import org.apache.arrow.plasma.ObjectStoreLink;
import org.apache.commons.lang3.NotImplementedException;
import org.ray.api.id.UniqueId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mock implementation of {@code org.ray.spi.ObjectStoreLink}, which use Map to store data.
 */
public class MockObjectStore implements ObjectStoreLink {

  private static final Logger LOGGER = LoggerFactory.getLogger(MockObjectStore.class);

  private static final int GET_CHECK_INTERVAL_MS = 100;

  private final Map<UniqueId, byte[]> data = new ConcurrentHashMap<>();
  private final Map<UniqueId, byte[]> metadata = new ConcurrentHashMap<>();

  private final List<Consumer<UniqueId>> objectPutCallbacks;

  public MockObjectStore() {
    this.objectPutCallbacks = new ArrayList<>();
  }

  public void addObjectPutCallback(Consumer<UniqueId> callback) {
    this.objectPutCallbacks.add(callback);
  }

  @Override
  public void put(byte[] objectId, byte[] value, byte[] metadataValue) {
    Preconditions.checkArgument(objectId != null && objectId.length != 0);
    Preconditions.checkArgument(value != null);
    data.put(new UniqueId(objectId), value);
    metadata.put(new UniqueId(objectId), metadataValue);
    UniqueId id = new UniqueId(objectId);
    for (Consumer<UniqueId> callback : objectPutCallbacks) {
      callback.accept(id);
    }
  }

  @Override
  public byte[] get(byte[] objectId, int timeoutMs, boolean isMetadata) {
    return get(new byte[][] {objectId}, timeoutMs, isMetadata).get(0);
  }

  @Override
  public List<byte[]> get(byte[][] objectIds, int timeoutMs, boolean isMetadata) {
    int ready = 0;
    int remainingTime = timeoutMs;
    while (ready < objectIds.length && remainingTime > 0) {
      int sleepTime = Math.min(remainingTime, GET_CHECK_INTERVAL_MS);
      try {
        Thread.sleep(sleepTime);
      } catch (InterruptedException e) {
        LOGGER.warn("Got InterruptedException while sleeping.");
      }
      remainingTime -= sleepTime;
      ready = 0;
      for (byte[] id : objectIds) {
        if (data.containsKey(new UniqueId(id))) {
          ready += 1;
        }
      }
    }
    List<byte[]> res = new ArrayList<>();
    if (!isMetadata) {
      for (byte[] id : objectIds) {
        res.add(data.get(new UniqueId(id)));
      }
    } else {
      for (byte[] id : objectIds) {
        res.add(metadata.get(new UniqueId(id)));
      }
    }
    return res;
  }

  @Override
  public List<ObjectStoreData> get(byte[][] objectIds, int timeoutMs) {
    return null;
  }

  @Override
  public byte[] hash(byte[] objectId) {
    throw new NotImplementedException("");
  }

  @Override
  public long evict(long numBytes) {
    return 0;
  }

  @Override
  public void release(byte[] objectId) {
    return;
  }

  @Override
  public void delete(byte[] bytes) {

  }

  @Override
  public boolean contains(byte[] objectId) {
    return data.containsKey(new UniqueId(objectId));
  }

  private String getUserTrace() {
    StackTraceElement[] stes = Thread.currentThread().getStackTrace();
    int k = 1;
    while (stes[k].getClassName().startsWith("org.ray")
        && !stes[k].getClassName().contains("test")) {
      k++;
    }
    return stes[k].getFileName() + ":" + stes[k].getLineNumber();
  }

  public boolean isObjectReady(UniqueId id) {
    return data.containsKey(id);
  }

  public void free(UniqueId id) {
    data.remove(id);
    metadata.remove(id);
  }

}
