package org.ray.core;

import org.joda.time.Interval;
import org.nustaq.serialization.FSTConfiguration;
import org.apache.commons.lang3.NotImplementedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.reflect.ParameterizedType;
import java.sql.*;
import static java.util.Arrays.asList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.*;



import org.apache.arrow.flatbuf.Binary;
import org.apache.arrow.memory.*;
import org.apache.arrow.vector.ipc.*;
import org.apache.arrow.vector.types.*;
import org.apache.arrow.vector.types.pojo.*;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * class used for Java object serialization/deserialization
 */
public class Serializer {
  private static boolean arrowEnabled = true;

  private static final ThreadLocal<FSTConfiguration> conf = ThreadLocal.withInitial(
    FSTConfiguration::createDefaultConfiguration);

  private static final ConcurrentHashMap<Class<?>, Function<Object, HashMap<String, String>>> serCallbackMap =
    new ConcurrentHashMap<Class<?>, Function<Object, HashMap<String, String>>>();
  private static final ConcurrentHashMap<Class<?>, Function<HashMap<String, String>, Object>> dserCallbackMap = 
    new ConcurrentHashMap<Class<?>, Function<HashMap<String, String>, Object>>();

  /**
  * Function used to switch serialization mode between Arrow and FST.
  * Call setAorrwEnabled(true) to switch to Arrow;
  * Call setAorrwEnabled(false) to switch to FST.
  * The default setting is FST.
  */
  public static void setArrwwEnabled(boolean enabled) {
    arrowEnabled = enabled;
  }

  /**
  * Get current serialization mode.
  * Return 'true' if Arrow, 'false' if FST.
  */
  public static boolean getArrowEnabled() {
    return arrowEnabled;
  }

  /**
  * General entrance to serialize a Java object using the current mode.
  * The mode can be checked via getArrowEnabled().
  */
  public static byte[] serialize(Object obj) {
    if(!arrowEnabled)
      return conf.get().asByteArray(obj);
    else
      return encodeWithArrow(obj);
  }

  /**
  * Serialize a Java object using FST.
  */
  private static byte[] encodeWithFST(Object obj) {
      return conf.get().asByteArray(obj);
  }

  /**
  * Serialize a Java object using FST.
  */
  private static byte[] encodeWithFST(Object obj, ClassLoader classLoader) {
    byte[] result;
    FSTConfiguration current = conf.get();
    if (classLoader != null && classLoader != current.getClassLoader()) {
      ClassLoader old = current.getClassLoader();
      current.setClassLoader(classLoader);
      result = current.asByteArray(obj);
      current.setClassLoader(old);
    } else {
      result = current.asByteArray(obj);
    }

    return result;
  }

  /**
  * General entrance to deserialize a Java object using the current mode.
  * The mode can be checked via getArrowEnabled().
  */
  public static <T> T deserialize(byte[] bs, Class<T> cls) {
    if(!arrowEnabled)
      return decodeWithFST(bs);
    else
      return decodeWithArrow(bs, cls);
  }

  /**
  * Deserialize a Java object using FST.
  */
  @SuppressWarnings("unchecked")
  private static <T> T decodeWithFST(byte[] bs) {
    return (T) conf.get().asObject(bs);
  }

  /**
  * Deserialize a Java object using FST.
  */
  @SuppressWarnings("unchecked")
  private static <T> T decodeWithFST(byte[] bs, ClassLoader classLoader) {
    Object object;
    FSTConfiguration current = conf.get();
    if (classLoader != null && classLoader != current.getClassLoader()) {
      ClassLoader old = current.getClassLoader();
      current.setClassLoader(classLoader);
      object = current.asObject(bs);
      current.setClassLoader(old);
    } else {
      object = current.asObject(bs);
    }
    return (T) object;
  }

  public static void setClassloader(ClassLoader classLoader) {
    conf.get().setClassLoader(classLoader);
  }

  /**
  * Register serialization/deserialization calbacks for a class.
  * Callbacks will essentially be called through callSerCallback and callDserCallback.
  * If you register the same type twice, the callbacks will not be updated.
  */
  public static void registerClass(Class<?> cls, Function<Object, HashMap<String, String>> serCallback, 
    Function<HashMap<String, String>, Object> dserCallback) {
      serCallbackMap.computeIfAbsent(cls, k -> serCallback);
      dserCallbackMap.computeIfAbsent(cls, k -> dserCallback);
  }

  /**
  * Serialize a Java object using Arrow.
  */
  private static byte[] encodeWithArrow(Object obj) {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    obj = callSerCallback(obj);
    Schema schema = getSchema(obj.getClass());
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
      VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator)) {
      ArrowWriter writer = ArrowWriter.create(root);
      writer.write(obj);
      writer.finish();
      writer.reset();
      ArrowStreamWriter streamWriter = new ArrowStreamWriter(root, null, stream);
      streamWriter.start();
      streamWriter.writeBatch();
      streamWriter.end();
      byte[] res = stream.toByteArray();
      streamWriter.close();
      root.close();
      return res;
    }
    catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  /**
  * Deserialize a Java object using Arrow.
  */
  @SuppressWarnings("unchecked")
  private static <T> T decodeWithArrow(byte[] bs, Class<T> cls) {
    ByteArrayInputStream in = new ByteArrayInputStream(bs);
    try (BufferAllocator allocator = new RootAllocator(Integer.MAX_VALUE);
      ArrowStreamReader reader = new ArrowStreamReader(in, allocator)) {
        Schema schema = reader.getVectorSchemaRoot().getSchema();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
        ArrowReader arrowReader = ArrowReader.create(root);
        Object obj = arrowReader.read();
        return (T) callDserCallback(obj, cls);
    }
    catch (Exception ex){
      throw new RuntimeException(ex);
    }
  }

  /**
  * Call registered callback before perform a serialization (from encodeWithArrow()).
  */
  private static Object callSerCallback(Object obj) {
    Class<?> cls = obj.getClass();
    if(serCallbackMap.containsKey(cls))
      return serCallbackMap.get(cls).apply(obj);
    else
      return obj;
  }

  /**
  * Call registered callback after perform a deserialization (from decodeWithArrow()).
  */
  @SuppressWarnings("unchecked")
  private static Object callDserCallback(Object obj, Class<?> cls) {
    if(dserCallbackMap.containsKey(cls))
      return dserCallbackMap.get(cls).apply((HashMap<String, String>)obj);
    else
      return obj;
  }

  /**
  * Function to create the Arrow schema for a general class.
  * The schema will be used to serialize an object.
  */
  private static Schema getSchema(Class<?> cls) {
    if (cls == null)
      return new Schema(asList(new Field("null", FieldType.nullable(ArrowType.Null.INSTANCE), null)));
    else if (cls.equals(Byte.class))
      return new Schema(asList(new Field("byte", FieldType.nullable(new ArrowType.Int(8, true)), null)));
    else if (cls.equals(Binary.class))
      return new Schema(asList(new Field("binary", FieldType.nullable(ArrowType.Binary.INSTANCE), null)));
    else if (cls.equals(Short.class))
      return new Schema(asList(new Field("short", FieldType.nullable(new ArrowType.Int(16, true)), null)));
    else if (cls.equals(Integer.class))
      return new Schema(asList(new Field("int", FieldType.nullable(new ArrowType.Int(32, true)), null)));
    else if (cls.equals(Long.class))
      return new Schema(asList(new Field("long", FieldType.nullable(new ArrowType.Int(64, true)), null)));
    else if (cls.equals(String.class))
      return new Schema(asList(new Field("string", FieldType.nullable(ArrowType.Utf8.INSTANCE), null)));
    else if (cls.equals(Float.class))
      return new Schema(asList(new Field("float", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)), null)));
    else if (cls.equals(Double.class))
      return new Schema(asList(new Field("double", FieldType.nullable(new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE)), null)));
    else if (cls.equals(Boolean.class))
      return new Schema(asList(new Field("bool", FieldType.nullable(ArrowType.Bool.INSTANCE), null)));
    else if (cls.equals(java.util.Date.class))
      return new Schema(asList(new Field("date", FieldType.nullable(new ArrowType.Date(DateUnit.MILLISECOND)), null)));
    else if (cls.equals(Timestamp.class))
      return new Schema(asList(new Field("tiemstamp", FieldType.nullable(new ArrowType.Timestamp(TimeUnit.MILLISECOND, "UTC")), null)));
    else if (cls.equals(Interval.class))
      return new Schema(asList(new Field("interval", FieldType.nullable(new ArrowType.Interval(IntervalUnit.DAY_TIME)), null)));
    else if (cls.equals(Array.class) || cls.equals(List.class)) {
      ParameterizedType pt = (ParameterizedType) cls.getGenericSuperclass();
      Class<?> childCls = (Class<?>) pt.getActualTypeArguments()[0];
      Schema childSchema = getSchema(childCls);
      return new Schema(asList(new Field("list", FieldType.nullable(ArrowType.List.INSTANCE), childSchema.getFields())));
    }
    else if (cls.equals(HashMap.class))
      throw new NotImplementedException("Map is not supported yet.");
    else
      throw new RuntimeException(String.format("Class '%s' is not supported yet.", cls));
  }
}
