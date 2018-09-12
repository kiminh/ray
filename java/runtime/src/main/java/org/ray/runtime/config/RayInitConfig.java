package org.ray.runtime.config;

import java.util.HashMap;
import java.util.Map;

public class RayInitConfig {
    private String redisIpAddr = "";
    private int redisPort = 0;
    private RunMode runMode = RunMode.SINGLE_BOX;
    private WorkerMode workerMode = WorkerMode.NONE;
    //TODO(qwang): We should change this member later to make it better.
    private Map<String, String> properties = new HashMap<>();

    public void setRedisAddr(String redisAddr) {
        //TODO(qwang): split iy by ':'
    }

    public void setRedisIpAddr(String redisIpAddr) {
        this.redisIpAddr = redisIpAddr;
    }

    public void setRedisPort(int redisPort) {
        this.redisPort = redisPort;
    }

    public void setRunMode(RunMode runMode) {
        this.runMode = runMode;
    }

    public void setWorkerMode(WorkerMode workerMode) {
        this.workerMode = workerMode;
    }

    public void setProperity(String key, String value) {
        properties.put(key, value);
    }

    public String getProperity(String key) {
        return properties.get(key);
    }

}
