package org.ray.streaming.runtime.config.global;

import org.aeonbits.owner.Config;

/**
 * Alipay.com Inc Copyright (c) 2004-2019 All Rights Reserved.
 *
 * @author yangjianzhang on 2019-12-12.
 */
public interface StateBackendConfig extends Config {

  String STATE_BACKEND_TYPE = "streaming.state-backend.type";

  @DefaultValue(value = "memory")
  @Key(value = STATE_BACKEND_TYPE)
  String stateBackendType();

}
