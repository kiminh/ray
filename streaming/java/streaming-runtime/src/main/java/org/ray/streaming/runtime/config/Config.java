package org.ray.streaming.runtime.config;

import java.io.Serializable;

import javax.accessibility.Accessible;

import org.ray.streaming.runtime.config.converter.EnvConverter;

/**
 * Basic config interface.
 */
public interface Config extends org.aeonbits.owner.Config, Accessible, Serializable {

    @DefaultValue("dev")
    @ConverterClass(EnvConverter.class)
    @Key(value = "streaming.env")
    String env();
}
