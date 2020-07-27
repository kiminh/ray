package io.ray.runtime.actor;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.runtime.generated.Common.Language;
import io.ray.runtime.id.ActorId;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Abstract and language-independent implementation of actor handle for cluster mode. This is a
 * wrapper class for C++ ActorHandle.
 */
public abstract class NativeActorHandle extends BaseActorHandleImpl implements BaseActorHandle,
  Externalizable {

  private Language language;

  NativeActorHandle(ActorId actorId, Language language) {
    super(actorId);
    Preconditions.checkState(!actorId.isNil());
    this.language = language;
  }

  /**
   * Required by FST
   */
  NativeActorHandle() {
    super(ActorId.NIL);
  }

  public static NativeActorHandle create(ActorId actorId) {
    Language language = Language.forNumber(nativeGetLanguage(actorId.getBytes()));
    Preconditions.checkState(language != null, "Language shouldn't be null");
    return create(actorId, language);
  }

  public static NativeActorHandle create(ActorId actorId, Language language) {
    switch (language) {
      case JAVA:
        return new NativeJavaActorHandle(actorId);
      case PYTHON:
        return new NativePyActorHandle(actorId);
      default:
        throw new IllegalStateException("Unknown actor handle language: " + language);
    }
  }

  public Language getLanguage() {
    return language;
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeObject(nativeSerialize(actorId.getBytes()));
    out.writeObject(language);
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    actorId = ActorId.fromBytes(nativeDeserialize((byte[]) in.readObject()));
    language = (Language) in.readObject();
  }

  /**
   * Serialize this actor handle to bytes.
   *
   * @return  the bytes of the actor handle
   */
  public byte[] toBytes() {
    return nativeSerialize(actorId.getBytes());
  }

  /**
   * Deserialize an actor handle from bytes.
   *
   * @return  the bytes of an actor handle
   */
  public static NativeActorHandle fromBytes(byte[] bytes) {
    byte[] actorId = nativeDeserialize(bytes);
    Language language = Language.forNumber(nativeGetLanguage(actorId));
    Preconditions.checkNotNull(language);
    return create(ActorId.fromBytes(actorId), language);
  }

  // TODO(chaokunyang) do we need to free the ActorHandle in core worker by using phantom reference?

  private static native int nativeGetLanguage(byte[] actorId);

  static native List<String> nativeGetActorCreationTaskFunctionDescriptor(byte[] actorId);

  private static native byte[] nativeSerialize(byte[] actorId);

  private static native byte[] nativeDeserialize(byte[] data);
}
