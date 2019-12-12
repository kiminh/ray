package org.ray.streaming.runtime.config;

import com.google.common.base.Preconditions;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import org.aeonbits.owner.Config.DefaultValue;
import org.aeonbits.owner.Config.Key;
import org.aeonbits.owner.ConfigFactory;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.ray.streaming.runtime.config.global.LogConfig;
import org.ray.streaming.runtime.config.global.SerializationConfig;
import org.ray.streaming.runtime.config.global.StateBackendConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Alipay.com Inc
 * Copyright (c) 2004-2019 All Rights Reserved.
 *
 * @author yangjianzhang on 2019-12-12.
 */
public class StreamingGlobalConfig {
  private static final Logger LOG = LoggerFactory.getLogger(StreamingGlobalConfig.class);

  public CommonConfig commonConfig;
  public LogConfig logConfig;
  public SerializationConfig serializationConfig;
  public StateBackendConfig stateBackendConfig;

  public Map<String, String> configMap = new HashMap<>();

  public StreamingGlobalConfig(Map<String, String> conf) {
    commonConfig = ConfigFactory.create(CommonConfig.class, conf);
    logConfig = ConfigFactory.create(LogConfig.class, conf);
    serializationConfig = ConfigFactory.create(SerializationConfig.class, conf);
    stateBackendConfig = ConfigFactory.create(StateBackendConfig.class, conf);
    globalConfig2Map();
  }

  public void globalConfig2Map() {
    try {
      configMap.putAll(config2Map(this.commonConfig));
      configMap.putAll(config2Map(this.logConfig));
      configMap.putAll(config2Map(this.serializationConfig));
    } catch (Exception e) {
      LOG.error("Global config to map occur error.", e);
    }
  }

  public Map<String, String> config2Map(org.aeonbits.owner.Config config) throws ClassNotFoundException {
    Map<String, String> result = new HashMap<>();

    Class<?> proxyClazz = Class.forName(config.getClass().getName());
    Class<?>[] proxyInterfaces = proxyClazz.getInterfaces();

    Class<?> configInterface = null;
    for (Class<?> proxyInterface : proxyInterfaces) {
      if (Config.class.isAssignableFrom(proxyInterface)) {
        configInterface = proxyInterface;
        break;
      }
    }
    assert configInterface != null;
    Preconditions.checkArgument(configInterface != null,
        "Can not get config interface.");
    Method[] methods = configInterface.getMethods();

    for (Method method : methods) {
      Key ownerKeyAnnotation = method.getAnnotation(Key.class);
      String ownerKeyAnnotationValue;
      if (ownerKeyAnnotation != null) {
        ownerKeyAnnotationValue = ownerKeyAnnotation.value();
        Object value;
        try {
          value = method.invoke(config);
        } catch (Exception e) {
          LOG.warn("Can not get value by method invoking for config key: {}. "
              + "So use default value instead.", ownerKeyAnnotationValue);
          String defaultValue = method.getAnnotation(DefaultValue.class).value();
          value = defaultValue;
        }
        result.put(ownerKeyAnnotationValue, value + "");
      }
    }
    return result;
  }
}
