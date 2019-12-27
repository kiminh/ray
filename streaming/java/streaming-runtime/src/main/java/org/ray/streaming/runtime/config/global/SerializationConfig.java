package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;

/**
 * Job serializer config.
 */
public interface SerializationConfig extends Config {

  String SERIALIZER_TYPE = "streaming.serializer.type";

  @DefaultValue(value = "fury")
  @Key(value = SERIALIZER_TYPE)
  String serializerType();
}
