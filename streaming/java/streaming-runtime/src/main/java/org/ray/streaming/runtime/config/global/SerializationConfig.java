package org.ray.streaming.runtime.config.global;

import org.ray.streaming.runtime.config.Config;

/**
 * Alipay.com Inc Copyright (c) 2004-2019 All Rights Reserved.
 *
 * @author yangjianzhang on 2019-12-12.
 */
public interface SerializationConfig extends Config {

  String SERIALIZER_TYPE = "streaming.serializer.type";

  @DefaultValue(value = "fury")
  @Key(value = SERIALIZER_TYPE)
  String serializerType();

}
