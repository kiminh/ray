package org.ray.streaming.runtime.config;

import java.io.Serializable;

import javax.accessibility.Accessible;

import org.ray.streaming.runtime.config.converter.EnvConverter;

/**
 *
 */
public interface Config extends org.aeonbits.owner.Config, Accessible, Serializable {

    @DefaultValue("dev")
    @ConverterClass(EnvConverter.class)
    @Key(value = "Streaming.env")
    String env();

}
