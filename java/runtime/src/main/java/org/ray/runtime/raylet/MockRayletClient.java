package org.ray.runtime.raylet;

import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ray.api.RayObject;
import org.ray.api.WaitResult;
import org.ray.api.id.UniqueId;
import org.ray.runtime.RayDevRuntime;
import org.ray.runtime.objectstore.MockObjectStore;
import org.ray.runtime.task.FunctionArg;
import org.ray.runtime.task.TaskSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A mock implementation of RayletClient, used in single process mode.
 */
public class MockRayletClient implements RayletClient {
  private static final Logger LOGGER = LoggerFactory.getLogger(MockRayletClient.class);

  private final Map<UniqueId, Set<TaskSpec>> waitingTasks = new ConcurrentHashMap<>();
  private final MockObjectStore store;
  private final RayDevRuntime runtime;
  private final ExecutorService exec;

  public MockRayletClient(RayDevRuntime runtime, int numberThreads) {
    this.runtime = runtime;
    this.store = runtime.getObjectStore();
    store.addObjectPutCallback(this::onObjectPut);
    // the thread pool that executes tasks in parallel
    exec = Executors.newFixedThreadPool(numberThreads);
  }

  public void onObjectPut(UniqueId id) {
    LOGGER.debug("Object {} is ready.", id);
    Set<TaskSpec> tasks = waitingTasks.get(id);
    if (tasks != null) {
      waitingTasks.remove(id);
      for (TaskSpec taskSpec : tasks) {
        submitTask(taskSpec);
      }
    }
  }

  @Override
  public void submitTask(TaskSpec task) {
    LOGGER.debug("Submitting task: {}.", task);
    Set<UniqueId> unreadyObjects = getUnreadyObjects(task);
    if (unreadyObjects.isEmpty()) {
      // If all dependencies are ready, execute this task.
      exec.submit(() -> {
        runtime.getWorker().execute(task);
        // If the task is an actor task or an actor creation task,
        // put the dummy object in object store, so those tasks which depends on it can be executed.
        if (task.isActorCreationTask() || task.isActorTask()) {
          UniqueId[] returnIds = task.returnIds;
          store.put(returnIds[returnIds.length - 1].getBytes(),
                  new byte[]{}, new byte[]{});
        }
      });
    } else {
      // If some dependencies aren't ready yet, put this task in waiting list.
      for (UniqueId id : unreadyObjects) {
        waitingTasks.computeIfAbsent(id, k -> new HashSet<>()).add(task);
      }
    }
  }

  private Set<UniqueId> getUnreadyObjects(TaskSpec spec) {
    Set<UniqueId> unreadyObjects = new HashSet<>();
    // check whether the arguments which this task needs is ready
    for (FunctionArg arg : spec.args) {
      if (arg.id != null) {
        if (!store.isObjectReady(arg.id)) {
          // if this objectId doesn't exist in store, then return this objectId
          unreadyObjects.add(arg.id);
        }
      }
    }
    // check whether the dependencies which this task needs is ready
    for (UniqueId id : spec.getExecutionDependencies()) {
      if (!store.isObjectReady(id)) {
        unreadyObjects.add(id);
      }
    }
    return unreadyObjects;
  }

  @Override
  public TaskSpec getTask() {
    throw new RuntimeException("invalid execution flow here");
  }

  @Override
  public void fetchOrReconstruct(List<UniqueId> objectIds, boolean fetchOnly,
      UniqueId currentTaskId) {

  }

  @Override
  public void notifyUnblocked(UniqueId currentTaskId) {

  }

  @Override
  public UniqueId generateTaskId(UniqueId driverId, UniqueId parentTaskId, int taskIndex) {
    return UniqueId.randomId();
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int
      timeoutMs, UniqueId currentTaskId) {
    if (waitList == null || waitList.isEmpty()) {
      return new WaitResult<>(ImmutableList.of(), ImmutableList.of());
    }
    byte[][] ids = new byte[waitList.size()][];
    for (int i = 0; i < waitList.size(); i++) {
      ids[i] = waitList.get(i).getId().getBytes();
    }
    List<RayObject<T>> readyList = new ArrayList<>();
    List<RayObject<T>> unreadyList = new ArrayList<>();
    List<byte[]> result = store.get(ids, timeoutMs, false);
    for (int i = 0; i < waitList.size(); i++) {
      if (result.get(i) != null) {
        readyList.add(waitList.get(i));
      } else {
        unreadyList.add(waitList.get(i));
      }
    }
    return new WaitResult<>(readyList, unreadyList);
  }

  @Override
  public void freePlasmaObjects(List<UniqueId> objectIds, boolean localOnly) {
    for (UniqueId id : objectIds) {
      store.free(id);
    }
  }

  @Override
  public void destroy() {
    exec.shutdown();
  }
}
