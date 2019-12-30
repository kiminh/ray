package org.ray.streaming.runtime.streamingqueue;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

import org.ray.streaming.runtime.config.StreamingWorkerConfig;
import org.ray.streaming.runtime.config.global.CommonConfig;
import org.ray.streaming.runtime.config.global.TransferConfig;
import org.ray.streaming.runtime.config.types.TransferChannelType;
import org.ray.streaming.runtime.transfer.DataMessage;
import org.ray.streaming.runtime.transfer.DataReader;

@RayRemote
public class ReaderWorker extends Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReaderWorker.class);

  private String name;
  private List<String> inputQueueList = null;
  private List<ActorId> inputActorIds = new ArrayList<>();
  private DataReader dataReader = null;
  private long handler = 0;
  private RayActor peerActor = null;
  private int msgCount = 0;
  private int totalMsg = 0;

  public ReaderWorker(String name) {
    LOGGER.info("ReaderWorker constructor");
    this.name = name;
  }

  public String getName() {
    String management = ManagementFactory.getRuntimeMXBean().getName();
    String pid = management.split("@")[0];

    LOGGER.info("pid: {} name: {}", pid, name);
    return name;
  }

  public String testRayCall() {
    LOGGER.info("testRayCall called");
    return "testRayCall";
  }

  public boolean init(List<String> inputQueueList, RayActor peer, int msgCount) {

    this.inputQueueList = inputQueueList;
    this.peerActor = peer;
    this.msgCount = msgCount;

    LOGGER.info("ReaderWorker init");
    LOGGER.info("java.library.path = {}", System.getProperty("java.library.path"));

    for (String queue : this.inputQueueList) {
      inputActorIds.add(this.peerActor.getId());
      LOGGER.info("ReaderWorker actorId: {}", this.peerActor.getId());
    }

    Map<String, String> conf = new HashMap<>();

    conf.put(TransferConfig.CHANNEL_TYPE, TransferChannelType.NATIVE_CHANNEL.name());
    conf.put(TransferConfig.CHANNEL_SIZE, "100000");
    conf.put(CommonConfig.JOB_NAME, "integrationTest1");

    dataReader = new DataReader(inputQueueList, inputActorIds,
        new StreamingWorkerConfig(conf));

    // Should not GetBundle in RayCall thread
    Thread readThread = new Thread(Ray.wrapRunnable(new Runnable() {
      @Override
      public void run() {
        consume();
      }
    }));
    readThread.start();

    LOGGER.info("ReaderWorker init done");

    return true;
  }

  public final void consume() {

    int checkPointId = 1;
    for (int i = 0; i < msgCount * inputQueueList.size(); ++i) {
      DataMessage dataMessage = dataReader.read(100);

      if (dataMessage == null) {
        LOGGER.error("dataMessage is null");
        i--;
        continue;
      }

      int bufferSize = dataMessage.body().remaining();
      int dataSize = dataMessage.body().getInt();

      // check size
      LOGGER.info("capacity {} bufferSize {} dataSize {}", dataMessage.body().capacity(), bufferSize, dataSize);
      Assert.assertEquals(bufferSize, dataSize);
      if (dataMessage instanceof DataMessage) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("{} : {}  message.", i, dataMessage.toString());
        }
        // check content
        for (int j = 0; j < dataSize - 4; ++j) {
          Assert.assertEquals(dataMessage.body().get(), (byte) j);
        }
      } else {
        LOGGER.error("unknown message type");
        Assert.fail();
      }

      totalMsg++;
    }

    LOGGER.info("ReaderWorker consume data done.");
  }

  void onQueueTransfer(long handler, byte[] buffer) {
  }


  public boolean done() {
    return totalMsg == msgCount;
  }

  public int getTotalMsg() {
    return totalMsg;
  }
}
