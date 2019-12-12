package org.ray.streaming.runtime.config.worker;

import org.aeonbits.owner.Config;

/**
 *
 */
public interface ExtraResourceConfig extends Config {
  String EXTRA_RESOURCE_URL = "streaming.extra-resource.url";
  String EXTRA_RESOURCE_TIMEOUT = "streaming.extra-resource.timeout";
  String EXTRA_RESOURCE_TEMP_PATH = "streaming.extra-resource.temp.path";
  String EXTRA_RESOURCE_DRIVER_RESOURCE_TEMP_PATH = "streaming.extra-resource.driver.temp.path";
  String EXTRA_RESOURCE_TEMP_CONTENT_CONN = "streaming.extra-resource.temp.content.connector";

  @DefaultValue(value = "")
  @Key(value = EXTRA_RESOURCE_URL)
  String extraResourceUrl();

  @DefaultValue(value = "60000")
  @Key(value = EXTRA_RESOURCE_TIMEOUT)
  long extraResourceTimeout();

  @DefaultValue(value = "/tmp/streaming-extra-resource/")
  @Key(value = EXTRA_RESOURCE_TEMP_PATH)
  String extraResourceTempPath();

  @DefaultValue(value = "")
  @Key(value = EXTRA_RESOURCE_DRIVER_RESOURCE_TEMP_PATH)
  String extraResourceDriverResourceTempPath();

  @DefaultValue(value = "-")
  @Key(value = EXTRA_RESOURCE_TEMP_CONTENT_CONN)
  String extraResourceTempContentConnector();

}
