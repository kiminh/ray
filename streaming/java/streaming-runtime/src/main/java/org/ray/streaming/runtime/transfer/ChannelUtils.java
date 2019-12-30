package org.ray.streaming.runtime.transfer;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;

import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.generated.Streaming;
import org.ray.streaming.runtime.generated.Streaming.OperatorType;
import org.ray.streaming.runtime.util.LoggerFactory;

public class ChannelUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ChannelUtils.class);

  static byte[] toNativeConf(StreamingWorkerConfig workerConfig) {
    Streaming.StreamingConfig.Builder builder = Streaming.StreamingConfig.newBuilder();

    // job name
    String jobName = workerConfig.commonConfig.jobName();
    if (!StringUtils.isEmpty(jobName)) {
      builder.setJobName(workerConfig.commonConfig.jobName());
    }

    // job id
    String jobId = workerConfig.commonConfig.jobId();
    if (!StringUtils.isEmpty(jobId)) {
      builder.setTaskJobId(jobId);
    }

    // worker name
    String workerName = workerConfig.workerInternalConfig.workerName();
    if (!StringUtils.isEmpty(workerName)) {
      builder.setWorkerName(workerName);
    }

    // operator name
    String operatorName = workerConfig.workerInternalConfig.workerOperatorName();
    if (!StringUtils.isEmpty(operatorName)) {
      builder.setOpName(operatorName);
    }

    // operator type
    OperatorType operatorType = workerConfig.workerInternalConfig.workerType();
    builder.setRole(operatorType);

    // ring buffer capacity
    int ringBufferCapacity = workerConfig.transferConfig.ringBufferCapacity();
    if (ringBufferCapacity != -1) {
      builder.setRingBufferCapacity(ringBufferCapacity);
    }

    // empty message interval
    int emptyMsgInterval = workerConfig.transferConfig.emptyMsgInterval();
    if (emptyMsgInterval != -1) {
      builder.setEmptyMessageInterval(emptyMsgInterval);
    }
    Streaming.StreamingConfig streamingConf = builder.build();
    LOG.info("Streaming native conf is: {}", streamingConf.toString());
    return streamingConf.toByteArray();
  }
}

