package org.ray.streaming.runtime.config;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Configuration implements java.io.Serializable, Cloneable {

  private static final Logger LOG = LoggerFactory.getLogger(Configuration.class);

  protected final Map<String, Object> confData;

  public Configuration() {
    this.confData = new HashMap<>();
  }

  public Configuration(Map<String, Object> confData) {
    this.confData = new HashMap<>();
    this.confData.putAll(confData);
  }

  public String getString(String key, String defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    } else {
      return o.toString();
    }
  }

  public void setString(String key, String value) {
    setValueInternal(key, value);
  }

  public int getInteger(String key, int defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    }
    return convertToInt(o, defaultValue);
  }

  public void setInteger(String key, int value) {
    setValueInternal(key, value);
  }

  public long getLong(String key, long defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    }
    return convertToLong(o, defaultValue);
  }

  public void setLong(String key, long value) {
    setValueInternal(key, value);
  }

  public boolean getBoolean(String key, boolean defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    }
    return convertToBoolean(o);
  }

  public void setBoolean(String key, boolean value) {
    setValueInternal(key, value);
  }

  public float getFloat(String key, float defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    }
    return convertToFloat(o, defaultValue);
  }

  public void setFloat(String key, float value) {
    setValueInternal(key, value);
  }

  public double getDouble(String key, double defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    }
    return convertToDouble(o, defaultValue);
  }

  public void setDouble(String key, double value) {
    setValueInternal(key, value);
  }

  @SuppressWarnings("EqualsBetweenInconvertibleTypes")
  public byte[] getBytes(String key, byte[] defaultValue) {
    Object o = getRawValue(key);
    if (o == null) {
      return defaultValue;
    }
    else if (o.getClass().equals(byte[].class)) {
      return (byte[]) o;
    }
    else {
      LOG.warn("Configuration cannot evaluate value {} as a byte[] value", o);
      return defaultValue;
    }
  }

  public void setBytes(String key, byte[] bytes) {
    setValueInternal(key, bytes);
  }

  public Set<String> keySet() {
    synchronized (this.confData) {
      return new HashSet<>(this.confData.keySet());
    }
  }

  public void addAll(Configuration other) {
    synchronized (this.confData) {
      synchronized (other.confData) {
        this.confData.putAll(other.confData);
      }
    }
  }

  public boolean containsKey(String key){
    synchronized (this.confData){
      return this.confData.containsKey(key);
    }
  }

  public Map<String, Object> toMap() {
    synchronized (this.confData){
      return this.confData;
    }
  }

  public Map<String, String> toStringMap() {
    Map<String, String> confStrMao = new HashMap<>();
    Map<String, Object> confMap = this.toMap();
    for (Map.Entry<String, Object> entry : confMap.entrySet()) {
      if (entry.getValue() instanceof String) {
        confStrMao.put(entry.getKey(), (String) entry.getValue());
      }
    }
    return confStrMao;
  }

  private int convertToInt(Object o, int defaultValue) {
    if (o.getClass() == Integer.class) {
      return (Integer) o;
    }
    else if (o.getClass() == Long.class) {
      long value = (Long) o;
      if (value <= Integer.MAX_VALUE && value >= Integer.MIN_VALUE) {
        return (int) value;
      } else {
        LOG.warn("Configuration value {} overflows/underflows the integer type.", value);
        return defaultValue;
      }
    }
    else {
      try {
        return Integer.parseInt(o.toString());
      }
      catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate value {} as an integer number", o);
        return defaultValue;
      }
    }
  }

  private long convertToLong(Object o, long defaultValue) {
    if (o.getClass() == Long.class) {
      return (Long) o;
    }
    else if (o.getClass() == Integer.class) {
      return ((Integer) o).longValue();
    }
    else {
      try {
        return Long.parseLong(o.toString());
      }
      catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate value " + o + " as a long integer number");
        return defaultValue;
      }
    }
  }

  private boolean convertToBoolean(Object o) {
    if (o.getClass() == Boolean.class) {
      return (Boolean) o;
    }
    else {
      return Boolean.parseBoolean(o.toString());
    }
  }

  private float convertToFloat(Object o, float defaultValue) {
    if (o.getClass() == Float.class) {
      return (Float) o;
    }
    else if (o.getClass() == Double.class) {
      double value = ((Double) o);
      if (value == 0.0
          || (value >= Float.MIN_VALUE && value <= Float.MAX_VALUE)
          || (value >= -Float.MAX_VALUE && value <= -Float.MIN_VALUE)) {
        return (float) value;
      } else {
        LOG.warn("Configuration value {} overflows/underflows the float type.", value);
        return defaultValue;
      }
    }
    else {
      try {
        return Float.parseFloat(o.toString());
      }
      catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate value {} as a float value", o);
        return defaultValue;
      }
    }
  }

  private double convertToDouble(Object o, double defaultValue) {
    if (o.getClass() == Double.class) {
      return (Double) o;
    }
    else if (o.getClass() == Float.class) {
      return ((Float) o).doubleValue();
    }
    else {
      try {
        return Double.parseDouble(o.toString());
      }
      catch (NumberFormatException e) {
        LOG.warn("Configuration cannot evaluate value {} as a double value", o);
        return defaultValue;
      }
    }
  }

  <T> void setValueInternal(String key, T value) {
    if (key == null) {
      throw new NullPointerException("Key must not be null.");
    }
    if (value == null) {
      throw new NullPointerException("Value must not be null.");
    }

    synchronized (this.confData) {
      this.confData.put(key, value);
    }
  }

  private Object getRawValue(String key) {
    if (key == null) {
      throw new NullPointerException("Key must not be null.");
    }

    synchronized (this.confData) {
      return this.confData.get(key);
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Configuration that = (Configuration) o;
    return Objects.equal(confData, that.confData);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(confData);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("confData", confData)
        .toString();
  }
}
