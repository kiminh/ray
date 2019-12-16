package org.ray.streaming.runtime.core.state.serde.impl;

import com.alipay.streaming.runtime.utils.LoggerFactory;
import com.alipay.streaming.runtime.utils.Md5Util;
import com.google.common.base.Preconditions;
import java.io.Serializable;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

public abstract class AbstractSerDe implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(AbstractSerDe.class);

  private final String keyPrefix;

  public AbstractSerDe(String keyPrefix) {
    this.keyPrefix = Preconditions.checkNotNull(keyPrefix, "keyPrefix");
    LOGGER.info("Key prefix is {}.", keyPrefix);
  }

  protected String generateRowKeyWithPrefix(String key) {
    if (StringUtils.isNotEmpty(key)) {
      String md5 = Md5Util.md5sum(key);
      if ("".equals(md5)) {
        throw new IllegalArgumentException("Invalid value to md5:" + key);
      }

      if (keyPrefix.isEmpty()) {
        return StringUtils.substring(md5, 0, 4) + ":" + key;
      }
      return StringUtils.substring(md5, 0, 4) + ":" + keyPrefix + ":" + key;
    } else {
      LOGGER.warn("key is empty.");
      return key;
    }
  }
}
