package org.ray.runtime.serializer;

import org.msgpack.core.MessageBufferPacker;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.*;

import java.io.IOException;
import java.lang.reflect.Array;
import java.math.BigInteger;
import java.util.Map;


public class MessagePackSerializer {
  private final static byte languageSpecificExtensionId = 101;


  private static void pack(Object object, MessagePacker packer, ClassLoader classLoader) throws IOException {
    if (object == null) {
      packer.packNil();
    } else if (object instanceof Byte) {
      packer.packByte((Byte) object);
    } else if (object instanceof Boolean) {
      packer.packBoolean((Boolean) object);
    } else if (object instanceof Double) {
      packer.packDouble((Double) object);
    } else if (object instanceof Float) {
      packer.packFloat((Float) object);
    } else if (object instanceof Integer) {
      packer.packInt((Integer) object);
    } else if (object instanceof Long) {
      packer.packLong((Long) object);
    } else if (object instanceof Short) {
      packer.packShort((Short) object);
    } else if (object instanceof BigInteger) {
      packer.packBigInteger((BigInteger) object);
    } else if (object instanceof String) {
      packer.packString((String) object);
    } else if (object instanceof byte[]) {
      byte[] bytes = (byte[]) object;
      packer.packBinaryHeader(bytes.length);
      packer.writePayload(bytes);
    } else if (object.getClass().isArray()) {
      int length = Array.getLength(object);
      packer.packArrayHeader(length);
      for (int i = 0; i < length; ++i) {
        pack(Array.get(object, i), packer, classLoader);
      }
      // We can't pack Map by MessagePack, because we don't know the key / value type class when unpacking.
//    } else if (object instanceof Map) {
//      packer.packMapHeader(((Map) object).size());
//      ((Map) object).forEach((k, v) -> {
//        try {
//          pack(k, packer, classLoader);
//          pack(v, packer, classLoader);
//        } catch (Exception ex) {
//          throw new RuntimeException(ex);
//        }
//      });
    } else {
      byte[] payload;
      if (classLoader == null) {
        payload = FSTSerializer.encode(object);
      } else {
        payload = FSTSerializer.encode(object, classLoader);
      }
      packer.packExtensionTypeHeader(languageSpecificExtensionId, payload.length);
      packer.addPayload(payload);
    }
  }

  private static Object unpack(Value v, Class<?> type, ClassLoader classLoader) {
    switch (v.getValueType()) {
      case NIL:
        return null;
      case BOOLEAN:
        if (type.isAssignableFrom(Boolean.class) || type.isAssignableFrom(boolean.class)) {
          return v.asBooleanValue().getBoolean();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Boolean!");
        }
      case INTEGER:
        IntegerValue iv = v.asIntegerValue();
        if (iv.isInByteRange() && (type.isAssignableFrom(Byte.class) || type.isAssignableFrom(byte.class))) {
          return iv.asByte();
        } else if (iv.isInShortRange() && (type.isAssignableFrom(Short.class) || type.isAssignableFrom(short.class))) {
          return iv.asShort();
        } else if (iv.isInIntRange() && (type.isAssignableFrom(Integer.class) || type.isAssignableFrom(int.class))) {
          return iv.asInt();
        } else if (iv.isInLongRange() && (type.isAssignableFrom(Long.class) || type.isAssignableFrom(long.class))) {
          return iv.asLong();
        } else if (type.isAssignableFrom(BigInteger.class)) {
          return iv.asBigInteger();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Integer!");
        }
      case FLOAT:
        if (type.isAssignableFrom(Double.class) || type.isAssignableFrom(double.class)) {
          return v.asFloatValue().toDouble(); // use as double
        } else if (type.isAssignableFrom(Float.class) || type.isAssignableFrom(float.class)) {
          return v.asFloatValue().toFloat();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Float!");
        }
      case STRING:
        if (type.isAssignableFrom(String.class)) {
          return v.asStringValue().asString();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual String!");
        }
      case BINARY:
        if (type.isAssignableFrom(byte[].class)) {
          return v.asBinaryValue().asByteArray();
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual byte[]!");
        }
      case ARRAY:
        if (type.isArray() || type.isAssignableFrom(Object.class)) {
          ArrayValue a = v.asArrayValue();
          Class<?> componentType = type.isArray() ? type.getComponentType() : Object.class;
          Object array = Array.newInstance(componentType, a.size());
          for (int i = 0; i < a.size(); ++i) {
            Value value = a.get(i);
            Array.set(array, i, unpack(value, componentType, classLoader));
          }
          return array;
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual Array!");
        }
      case EXTENSION:
        ExtensionValue ev = v.asExtensionValue();
        byte extType = ev.getType();
        if (extType == languageSpecificExtensionId) {
          if (classLoader == null) {
            return FSTSerializer.decode(ev.getData());
          } else {
            return FSTSerializer.decode(ev.getData(), classLoader);
          }
        } else {
          throw new IllegalArgumentException("expected " + type + ", actual extension type " + extType);
        }
    }
    throw new IllegalArgumentException("expected " + type + ", actual type " + v.getValueType());
  }

  public static byte[] encode(Object obj, ClassLoader classLoader) {
    MessageBufferPacker packer = MessagePack.newDefaultBufferPacker();
    try {
      pack(obj, packer, classLoader);
      return packer.toByteArray();
    } catch (Exception e) {
      throw new RuntimeException(e);
    } finally {
      packer.clear();
    }
  }


  @SuppressWarnings("unchecked")
  public static <T> T decode(byte[] bs, Class<?> type, ClassLoader classLoader) {
    try {
      MessageUnpacker unpacker = MessagePack.newDefaultUnpacker(bs);
      Value v = unpacker.unpackValue();
      type = type == null ? Object.class : type;
      return (T) unpack(v, type, classLoader);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public static void setClassloader(ClassLoader classLoader) {
    FSTSerializer.setClassloader(classLoader);
  }
}