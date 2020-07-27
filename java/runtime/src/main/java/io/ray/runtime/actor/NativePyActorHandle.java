package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.PyActorHandle;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.id.ActorId;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * Python actor handle implementation for cluster mode.
 */
public class NativePyActorHandle extends NativeActorHandle implements PyActorHandle {

  NativePyActorHandle(ActorId actorId) {
    super(actorId, Language.PYTHON);
  }

  /**
   * Required by FST
   */
  public NativePyActorHandle() {
    super();
  }

  @Override
  public String getModuleName() {
    return nativeGetActorCreationTaskFunctionDescriptor(actorId.getBytes()).get(0);
  }

  @Override
  public String getClassName() {
    return nativeGetActorCreationTaskFunctionDescriptor(actorId.getBytes()).get(1);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    super.readExternal(in);
    Preconditions.checkState(getLanguage() == Language.PYTHON);
  }
}
