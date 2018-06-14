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
      fargs[k] = new FunctionArg();
      if (oarg == null) {
        fargs[k].data = Serializer.encode(null);
      } else if (oarg.getClass().equals(RayActor.class)) {
        // serialize actor unique id
        if (k == 0) {
          RayActorId aid = new RayActorId();
          aid.id = ((RayActor) oarg).getId();
          fargs[k].data = Serializer.encode(aid);
        } else { // serialize actor handle
          fargs[k].data = Serializer.encode(oarg);
        }

      } else if (oarg.getClass().equals(RayObject.class)) {
        fargs[k].ids = new ArrayList<>();
        fargs[k].ids.add(((RayObject) oarg).getId());
      } else if (oarg instanceof RayMap) {
        fargs[k].ids = new ArrayList<>();
        RayMap<?, ?> rm = (RayMap<?, ?>) oarg;
        RayMapArg narg = new RayMapArg();
        for (Entry e : rm.EntrySet()) {
          narg.put(e.getKey(), ((RayObject) e.getValue()).getId());
          fargs[k].ids.add(((RayObject) e.getValue()).getId());
        }
        fargs[k].data = Serializer.encode(narg);
      } else if (oarg instanceof RayList) {
        fargs[k].ids = new ArrayList<>();
        RayList<?> rl = (RayList<?>) oarg;
        RayListArg narg = new RayListArg();
        for (RayObject e : rl.Objects()) {
          // narg.add(e.getId()); // we don't really need to use the ids
          fargs[k].ids.add(e.getId());
        }
        fargs[k].data = Serializer.encode(narg);
      } else if (checkSimpleValue(oarg)) {
        fargs[k].data = Serializer.encode(oarg);
      } else {
        //big parameter, use object store and pass future
        fargs[k].ids = new ArrayList<>();
        fargs[k].ids.add(RayRuntime.getInstance().put(oarg).getId());
      }
      k++;
    }
    return fargs;
  }

  private static boolean checkSimpleValue(Object o) {
    return true;//TODO I think Ray don't want to pass big parameter
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  public static Pair<Object, Object[]> unwrap(TaskSpec task, Method m, ClassLoader classLoader)
      throws TaskExecutionException {
    // the last arg is className

    FunctionArg[] fargs = Arrays.copyOf(task.args, task.args.length - 1);
    Object current = null;
    Object[] realArgs;

    int start = 0;

    // check actor method
    if (!Modifier.isStatic(m.getModifiers())) {
      start = 1;
      RayActorId actorId = Serializer.decode(fargs[0].data, classLoader);
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
        Object obj = Serializer.decode(farg.data, classLoader);

        // due to remote lambda, method may be static
        if (obj instanceof RayActorId) {
          assert (k == 0);
          realArgs[raIndex] = RayRuntime.getInstance().getLocalActor(((RayActorId) obj).id);
        } else {
          realArgs[raIndex] = obj;
        }
      } else if (farg.data == null) { // only ids, big data or single object id
        assert (farg.ids.size() == 1);
        realArgs[raIndex] = RayRuntime.getInstance().get(farg.ids.get(0));
      } else { // both id and data, could be RayList or RayMap only
        Object idBag = Serializer.decode(farg.data, classLoader);
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
