package org.ray.streaming.runtime.config.master;

import java.lang.reflect.Method;
import org.aeonbits.owner.ConfigFactory;
import org.aeonbits.owner.Converter;
import org.apache.commons.lang3.StringUtils;
import org.ray.streaming.runtime.util.EnvUtil;

/**
 *
 */
public class MaxParallelismConverter implements Converter<Integer> {

  private SchedulerConfig config = ConfigFactory.create(SchedulerConfig.class);

  @Override
  public Integer convert(Method method, String value) {
    if (!StringUtils.isEmpty(value)
        && config.maxParallelismProd() != Integer.parseInt(value)
        && config.maxParallelismDev() != Integer.parseInt(value)) {
      return Integer.parseInt(value);
    }
    if (EnvUtil.isOnlineEnv()) {
      return config.maxParallelismProd();
    }
    return config.maxParallelismDev();
  }
}
