package org.ray.yarn.config;


import java.io.Serializable;

/**
 * Define common actions of Config Classes
 */
abstract public class AbstractConfig implements Serializable {

  abstract public void validate();
}
