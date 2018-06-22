package org.ray.core;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.RayActor;
import org.ray.api.RayList;
import org.ray.api.RayMap;
import org.ray.api.RayObject;
import org.ray.api.UniqueID;
import org.ray.spi.model.FunctionArg;
import org.ray.spi.model.RayInvocation;
import org.ray.spi.model.TaskSpec;
import org.ray.util.exception.TaskExecutionException;

/**
 * arguments wrap and unwrap.
 */
public class ArgumentsBuilder {

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static FunctionArg[] wrap(RayInvocation invocation) {
    Object[] oargs = invocation.getArgs();
    FunctionArg[] fargs = new FunctionArg[oargs.length];
    int k = 0;
    for (Object oarg : oargs) {
      FunctionArg farg = new FunctionArg();
      if (oarg == null) {
        farg.data = Serializer.serialize(null);
      } else if (oarg.getClass().equals(RayObject.class)) {
        appendId(farg, ((RayObject) oarg).getId());
      } else if (oarg.getClass().equals(RayActor.class)) {
        if (k == 0) {
          // serialize actor id
          RayActorId aid = new RayActorId();
          aid.id = ((RayActor) oarg).getId();
          farg.data = Serializer.serialize(aid);
        } else {
          // serialize actor handle
          farg.data = Serializer.serialize(oarg);
        }
      } else if (oarg instanceof RayMap) {
        RayMap<?, ?> rm = (RayMap<?, ?>) oarg;
        RayMapArg narg = new RayMapArg();
        for (Entry e : rm.EntrySet()) {
          narg.put(e.getKey(), ((RayObject) e.getValue()).getId());
          appendId(farg, ((RayObject) e.getValue()).getId());
        }
        farg.data = Serializer.serialize(narg);
      } else if (oarg instanceof RayList) {
        RayList<?> rl = (RayList<?>) oarg;
        RayListArg narg = new RayListArg();
        for (RayObject e : rl.Objects()) {
          // narg.add(e.getId()); // we don't really need to use the ids
          appendId(farg, e.getId());
        }
        farg.data = Serializer.serialize(narg);
      } else if (isSimpleValue(oarg)) {
        farg.data = Serializer.serialize(oarg);
      } else {
        // big parameter, use object store and pass future
        appendId(farg, RayRuntime.getInstance().put(oarg).getId());
      }
      fargs[k++] = farg;
    }
    return fargs;
  }

  private static void appendId(FunctionArg arg, UniqueID id) {
    if(arg.ids == null) {
      arg.ids = new ArrayList<>();
    }
    arg.ids.add(id);
  }

  private static boolean isSimpleValue(Object o) {
    return true; //TODO I think Ray don't want to pass big parameter
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Pair<Object, Object[]> unwrap(TaskSpec task, Method m)
      throws TaskExecutionException {
    // the last arg is className

    FunctionArg[] fargs = Arrays.copyOf(task.args, task.args.length - 1);
    Object current = null;
    Object[] realArgs;

    int start = 0;

    // check actor method
    if (!Modifier.isStatic(m.getModifiers())) {
      start = 1;
      RayActorId actorId = Serializer.deserialize(fargs[0].data, RayActorId.class);
      current = RayRuntime.getInstance().getLocalActor(actorId.id);
      realArgs = new Object[fargs.length - 1];
    } else {
      realArgs = new Object[fargs.length];
    }

    int raIndex = 0;
    for (int k = start; k < fargs.length; k++, raIndex++) {
      FunctionArg farg = fargs[k];

      // pass by value
      if (farg.ids == null) {
        if(k == 0) {
          RayActorId obj = Serializer.deserialize(farg.data, RayActorId.class);
          realArgs[raIndex] = RayRuntime.getInstance().getLocalActor(obj.id);
        }
        else {
          Object obj = Serializer.deserialize(farg.data, Object.class);
          realArgs[raIndex] = obj;
        }
      } else if (farg.data == null) { // only ids, big data or single object id
        assert (farg.ids.size() == 1);
        realArgs[raIndex] = RayRuntime.getInstance().get(farg.ids.get(0));
      } else { // both id and data, could be RayList or RayMap only
        Object idBag = Serializer.deserialize(farg.data);
        if (idBag instanceof RayMapArg) {
          Map newMap = new HashMap<>();
          RayMapArg<?> oldmap = (RayMapArg<?>) idBag;
          assert (farg.ids.size() == oldmap.size());
          for (Entry<?, UniqueID> e : oldmap.entrySet()) {
            newMap.put(e.getKey(), RayRuntime.getInstance().get(e.getValue()));
          }
          realArgs[raIndex] = newMap;
        } else {
          List newlist = new ArrayList<>();
          for (UniqueID old : farg.ids) {
            newlist.add(RayRuntime.getInstance().get(old));
          }
          realArgs[raIndex] = newlist;
        }
      }
    }
    return Pair.of(current, realArgs);
  }

  //for recognition
  public static class RayMapArg<K> extends HashMap<K, UniqueID> {
    private static final long serialVersionUID = 8529310038241410256L;
  }

  //for recognition
  public static class RayListArg<K> extends ArrayList<K> {
    private static final long serialVersionUID = 8529310038241410256L;
  }

  public static class RayActorId implements Serializable {
    private static final long serialVersionUID = 3993646395842605166L;
    public UniqueID id;
  }
}
