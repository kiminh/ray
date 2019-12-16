package org.ray.streaming.runtime.rpc.call;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.slf4j.Logger;

import org.ray.streaming.runtime.util.LoggerFactory;
import org.ray.streaming.runtime.worker.JobWorker;
import org.ray.streaming.runtime.worker.JobWorkerContext;

public class RemoteCallWorker extends RemoteCallBase {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  public static RayObject<Boolean> initWorker(RayActor actor, JobWorkerContext ctx) {
    LOG.info("Call worker to init, actor: {}, context: {}.", actor.getId(), ctx);
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = Ray.call(JobWorker::init, actor, ctx);
    }

    LOG.info("Finish call worker to init.");
    return result;
  }

  public static RayObject<Boolean> startWorker(RayActor actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = Ray.call(JobWorker::start, actor);
    }

    LOG.info("Finish call worker to start.");
    return result;
  }

  public static RayObject<Boolean> destroyWorker(RayActor actor) {
    LOG.info("Call worker to destroy, actor is {}.", actor.getId());
    RayObject<Boolean> result = null;

    // python
    if (actor instanceof RayPyActor) {
      // TODO
    } else {
      // java
      result = Ray.call(JobWorker::destroy, actor);
    }

    LOG.info("Finish call worker to destroy.");
    return result;
  }

  private static List<Object> waitAndGetObjectList(
      Map<RayObject<Object>, RayActor> objectRayActorMap, int timeoutMs) {
    WaitResult<Object> waitRet = null;
    int waitingSize = objectRayActorMap.keySet().size();
    LOG.info("Wait and get object list size {}.", waitingSize);
    while (null == waitRet || waitRet.getReady().size() != waitingSize) {
      long currentTs = System.currentTimeMillis();
      waitRet =
          Ray.wait(objectRayActorMap.keySet().stream().collect(Collectors.toList()),
                   waitingSize,
                   timeoutMs);
      List<RayObject<Object>> unreadyList = waitRet.getUnready();
      if (unreadyList.size() > 0) {
        String unreadyActorsCombineStr =
            unreadyList.stream()
                .map(objId -> objectRayActorMap.get(objId).getId().toString())
                .reduce(" ", (str1, str2) -> str1 + "," + str2);
        LOG.warn("Unready size {}, list => {}", unreadyList.size(), unreadyActorsCombineStr);
        long tsDiff = System.currentTimeMillis() - currentTs;
        if (tsDiff < timeoutMs) {
          try {
            Thread.sleep(timeoutMs - tsDiff);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
        }
      } else {
        LOG.info("Waiting all successful.");
      }
    }
    return Ray.get(objectRayActorMap.keySet()
              .stream()
              .map(RayObject::getId)
              .collect(Collectors.toList()));
  }
}
