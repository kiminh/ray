package org.ray.streaming.runtime.config;

/**
 * Job execution environment.
 */
public enum  EnvironmentType {

    /**
     * DEV type.
     */
    DEV("dev"),

    /**
     * Prod type.
     */
    PROD("prod");

    private String name;

    EnvironmentType(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}
