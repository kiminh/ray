package org.ray.streaming.runtime.integrationtest;

import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.ray.api.Ray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.api.function.impl.ReduceFunction;
import org.ray.streaming.api.function.impl.SourceFunction;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.runtime.config.global.TransferConfig;
import org.ray.streaming.runtime.config.types.TransferChannelType;
import org.ray.streaming.runtime.util.TestHelper;

public class IntegrationTest {

  private static final Logger LOG = LoggerFactory.getLogger(IntegrationTest.class);

  private static Map<String, Integer> wordCountMap = new ConcurrentHashMap<>();

  @org.testng.annotations.BeforeClass
  public void setUp() {
    TestHelper.setUTPattern();
    Ray.init();
  }

  @org.testng.annotations.AfterClass
  public void tearDown() {
    TestHelper.clearUTPattern();
  }

  @org.testng.annotations.BeforeMethod
  public void testBegin(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " begin >>>>>>>>>>>>>>>>>>");
  }

  @org.testng.annotations.AfterMethod
  public void testEnd(Method method) {
    LOG.warn(">>>>>>>>>>>>>>>>>>>> Test case: " + method.getName() + " end >>>>>>>>>>>>>>>>>>");
  }

  @Test(timeOut = 120000)
  public void testStreamWordCount() throws Exception {
    TestHelper.setUTPattern();
    StreamingContext streamingContext = StreamingContext.buildContext();
    String jobName = "testStreamWordCount";

    Map<String, String> config = new HashMap<>();
    config.put(TransferConfig.CHANNEL_TYPE, TransferChannelType.MEMORY_CHANNEL.name());
    config.put(TransferConfig.CHANNEL_SIZE, "100000");
    streamingContext.withConfig(config);

    final int tot = 100;
    MySourceFunctionWithIndex function = new MySourceFunctionWithIndex("hello world", tot, 100);
    StreamSource<String> source = new StreamSource<>(streamingContext, function);
    source
        .flatMap((FlatMapFunction<String, WordAndCount>) (value, collector) -> {
          String[] records = value.split(" ");
          for (String record : records) {
            collector.collect(new WordAndCount(record, 1));
          }
        })
        .keyBy(pair -> pair.word)
        .reduce((ReduceFunction<WordAndCount>) (oldValue, newValue) -> {
          LOG.info("reduce: {} {}", oldValue, newValue);
          return new WordAndCount(oldValue.word, oldValue.count + newValue.count); })
        .sink(s -> {
          LOG.info("sink {} {}", s.word, s.count);
          wordCountMap.put(s.word, s.count);
        });

    streamingContext.execute(jobName);

    TimeUnit.SECONDS.sleep(3);

    while (wordCountMap.get("hello") < tot) {
      TimeUnit.MILLISECONDS.sleep(1000);
    }

    Assert.assertEquals(wordCountMap,
        ImmutableMap.of("hello", tot, "world", tot));

    TimeUnit.SECONDS.sleep(3);
    TestHelper.clearUTPattern();
  }

  static class MySourceFunctionWithIndex implements SourceFunction<String> {

    private int count;
    private String word;
    private int sleepTime;

    public MySourceFunctionWithIndex(String word, int count, int sleepTime) {
      this.word = word;
      this.count = count;
      this.sleepTime = sleepTime;
    }

    @Override
    public void init(int parallel, int index) {
      LOG.info("do init, parallel:{}, index:{}", parallel, index);
    }

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
      if (count > 0) {
        count--;
        TimeUnit.MILLISECONDS.sleep(sleepTime);
        LOG.info("Source output word: {}.", word);
        ctx.collect(word);
      }
    }

    @Override
    public void close() {
    }
  }

  static class WordAndCount implements Serializable {

    public final String word;
    public final Integer count;

    public WordAndCount(String key, Integer count) {
      this.word = key;
      this.count = count;
    }
  }
}
