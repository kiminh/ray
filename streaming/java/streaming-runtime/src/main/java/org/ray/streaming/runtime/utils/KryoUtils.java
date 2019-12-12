package org.ray.streaming.runtime.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Kryo.DefaultInstantiatorStrategy;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyListSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptyMapSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsEmptySetSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonListSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonMapSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.CollectionsSingletonSetSerializer;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSortedSetSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateTimeSerializer;
import java.lang.invoke.SerializedLambda;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.ray.streaming.runtime.config.StreamingConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class KryoUtils {
  private static final Logger LOG = LoggerFactory.getLogger(KryoUtils.class);

  private static final int BUFFER_SIZE = 4096 * 5;
  private static final String BUFFER_OVERFLOW_EXCEPTION_MESSAGE = "Buffer overflow";

  static class KryoThreadLocalContext {
    private Kryo kryo;
    private Input input;
    private Output output;
    private int    outputBufferSize = BUFFER_SIZE;

    public KryoThreadLocalContext(Kryo kryo, Input input, Output output) {
      this.kryo = kryo;
      this.input = input;
      this.output = output;
    }

    private Kryo getKryo() {
      return kryo;
    }

    public Input getInput() {
      return input;
    }

    public Output getOutput() {
      return output;
    }

    private Output resizeOutputBuffer() {
      LOG.info("Resize output buffer size from [{}] to [{}]."
          , outputBufferSize , outputBufferSize * 2);
      this.outputBufferSize *= 2;
      this.output = new Output(outputBufferSize);
      return this.output;
    }

  }

  // must be thread safely, see: https://github.com/EsotericSoftware/kryo#thread-safety
  private static final ThreadLocal<KryoThreadLocalContext> kryoThreadLocalContext = ThreadLocal.withInitial(() -> {
    Kryo kryo = createKryoInstance();
    Input input = new Input();
    Output output = new Output(BUFFER_SIZE);
    return new KryoThreadLocalContext(kryo, input, output);
  });


  static class KryoConfigSerializer extends com.esotericsoftware.kryo.Serializer<Config> {
    @Override
    public void write(Kryo kryo, Output output, Config config) {
      String renderResult = config.root().render(ConfigRenderOptions.concise());
      kryo.writeObjectOrNull(output, renderResult, String.class);
    }

    @Override
    public Config read(Kryo kryo, Input input, Class<Config> type) {
      String readResult = kryo.readObject(input, String.class);
      return ConfigFactory.parseString(readResult);
    }
  }

  static class KryoStreamingConfigSerializer extends
      com.esotericsoftware.kryo.Serializer<StreamingConfig> {
    @Override
    public void write(Kryo kryo, Output output, StreamingConfig config) {
      kryo.writeObjectOrNull(output, config.getMap(), HashMap.class);
    }

    @Override
    public StreamingConfig read(Kryo kryo, Input input, Class<StreamingConfig> type) {
      Map<String, String> map = kryo.readObject(input, HashMap.class);
      return new StreamingConfig(map);
    }
  }


  static class Tuple2<A, B> {
    private A a;
    private B b;
    public Tuple2() {
    }

    public Tuple2(A a, B b) {
      this.a = a;
      this.b = b;
    }

    public void setA(A a) {
      this.a = a;
    }

    public void setB(B b) {
      this.b = b;
    }

    public A getA() {
      return a;
    }

    public B getB() {
      return b;
    }
  }

  static class KryoArrayBlockingQueueSerializer extends
      com.esotericsoftware.kryo.Serializer<ArrayBlockingQueue> {

    @Override
    public void write(Kryo kryo, Output output, ArrayBlockingQueue queue) {
      Integer queueCapacity = queue.size() + queue.remainingCapacity();
      LinkedList list = new LinkedList(Arrays.asList(queue.toArray()));
      Tuple2<Integer, LinkedList> tuple2 = new Tuple2<>(queueCapacity, list);
      kryo.writeObjectOrNull(output, tuple2, Tuple2.class);
    }

    @Override
    public ArrayBlockingQueue read(Kryo kryo, Input input, Class<ArrayBlockingQueue> type) {
      Tuple2<Integer, LinkedList> tuple2 = kryo.readObject(input, Tuple2.class);
      ArrayBlockingQueue arrayBlockingQueue = new ArrayBlockingQueue(tuple2.a);
      tuple2.b.stream().forEach(
          obj -> {
            arrayBlockingQueue.add(obj);
          }
      );
      return arrayBlockingQueue;
    }
  }

  static class LinkedBlockingQueueSerializer extends Serializer<LinkedBlockingQueue> {

    @Override
    public void write(Kryo kryo, Output output, LinkedBlockingQueue queue) {
      Integer queueCapacity = queue.size() + queue.remainingCapacity();
      LinkedList list = new LinkedList(Arrays.asList(queue.toArray()));
      Tuple2<Integer, LinkedList> tuple2 = new Tuple2<>(queueCapacity, list);
      kryo.writeObjectOrNull(output, tuple2, Tuple2.class);
    }

    @Override
    public LinkedBlockingQueue read(Kryo kryo, Input input, Class<LinkedBlockingQueue> type) {
      Tuple2<Integer, LinkedList> tuple2 = kryo.readObject(input, Tuple2.class);
      LinkedBlockingQueue linkedBlockingQueue = new LinkedBlockingQueue(tuple2.a);
      tuple2.b.stream().forEach(
          obj -> {
            linkedBlockingQueue.add(obj);
          }
      );
      return linkedBlockingQueue;
    }
  }

  private static Kryo createKryoInstance() {
    Kryo kryo = new Kryo();

    kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    kryo.setReferences(true);
    kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));

    kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
    kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
    kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
    kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
    kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
    kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
    kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
    kryo.register(LocalDate.class, new JodaLocalDateSerializer());
    kryo.register(LocalDateTime.class, new JodaLocalDateTimeSerializer());
    kryo.register(Config.class, new KryoConfigSerializer());
    kryo.register(StreamingConfig.class, new KryoStreamingConfigSerializer());
    try {
      kryo.register(Class.forName("com.typesafe.config.impl.SimpleConfig"),
          new KryoConfigSerializer());
    } catch (ClassNotFoundException e) {
      // do nothing.
    }
    kryo.register(Object[].class);
    kryo.register(java.lang.Class.class);
    kryo.register(SerializedLambda.class);
    kryo.register(ClosureSerializer.Closure.class, new ClosureSerializer());
    kryo.register(ArrayBlockingQueue.class, new KryoArrayBlockingQueueSerializer());
    kryo.register(LinkedBlockingQueue.class, new LinkedBlockingQueueSerializer());


    ArrayListMultimapSerializer.registerSerializers(kryo);
    HashMultimapSerializer.registerSerializers(kryo);
    ImmutableListSerializer.registerSerializers(kryo);
    ImmutableMapSerializer.registerSerializers(kryo);
    ImmutableMultimapSerializer.registerSerializers(kryo);
    ImmutableSetSerializer.registerSerializers(kryo);
    ImmutableSortedSetSerializer.registerSerializers(kryo);
    LinkedHashMultimapSerializer.registerSerializers(kryo);
    LinkedListMultimapSerializer.registerSerializers(kryo);
    ReverseListSerializer.registerSerializers(kryo);
    TreeMultimapSerializer.registerSerializers(kryo);
    UnmodifiableNavigableSetSerializer.registerSerializers(kryo);

    UnmodifiableCollectionsSerializer.registerSerializers(kryo);
    SynchronizedCollectionsSerializer.registerSerializers(kryo);

//    kryo.register(NativeRayJavaActor.class, new KryoNativeRayActorSerializer());
//    kryo.register(NativeRayPyActor.class, new KryoNativeRayActorSerializer());

    return kryo;
  }

  /**
   * Get Kryo instance
   *
   * @return Kryo instance for current thread
   */
  public static Kryo getInstance() {
    return kryoThreadLocalContext.get().getKryo();
  }

  /**
   * Serialize obj to byte[]
   *
   * @param obj the input object
   * @param <T> the input object's type
   * @return byte[]
   */
  public static <T> byte[] writeToByteArray(T obj) {
    Output output = kryoThreadLocalContext.get().getOutput();
    while (true) {
      output.clear();
      try {
        Kryo kryo = getInstance();
        kryo.writeClassAndObject(output, obj);
        output.flush();
        break;
      } catch (KryoException e) {
        // need resize
        if (e.getMessage() != null && e.getMessage().startsWith(BUFFER_OVERFLOW_EXCEPTION_MESSAGE)) {
          output = kryoThreadLocalContext.get().resizeOutputBuffer();
        } else {
          throw e;
        }
      }
    }
    return output.toBytes();
  }

  /**
   * deserialize byte[] to object
   *
   * @param byteArray byte[]
   * @param <T>       object's type
   * @return object
   */
  public static <T> T readFromByteArray(byte[] byteArray) {
    Input input = kryoThreadLocalContext.get().getInput();
    input.setBuffer(byteArray);
    Kryo kryo = getInstance();
    return (T) kryo.readClassAndObject(input);
  }

  /**
   * clean up thread local
   */
  public static void clean() {
    kryoThreadLocalContext.remove();
  }

}
