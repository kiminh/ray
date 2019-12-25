package org.ray.streaming.runtime.config.converter;

import java.lang.reflect.Method;

import org.aeonbits.owner.Converter;

import org.ray.streaming.runtime.config.types.EnvironmentType;
import org.ray.streaming.runtime.util.EnvUtil;

/**
 *
 */
public class EnvConverter implements Converter<String> {

  @Override
  public String convert(Method method, String value) {
    if (EnvUtil.isOnlineEnv()) {
      return EnvironmentType.PROD.getName();
    } else {
      return EnvironmentType.DEV.getName();
    }
  }
}
