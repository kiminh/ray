package org.ray.streaming.nexmark;

import org.apache.beam.sdk.nexmark.model.Bid;
import org.kohsuke.args4j.Option;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.function.impl.FlatMapFunction;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.nexmark.sinks.DummyLatencyCountingSink;
import org.ray.streaming.nexmark.sources.BidSourceFunction;
import org.ray.streaming.util.Record4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

public class Query2 implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(Query2.class);

  @Option(name = "--exchange-rate", usage = "exchange-rate")
  private float exchangeRate = 0.82F;

  @Option(name = "--src-rate", usage = "srcRate")
  private int srcRate = 100000;

  public static void main(String[] args) throws Exception {
    new Query2().doMain(args);
  }

  public void doMain(String[] args) throws Exception {

    StreamingContext streamingContext = StreamingContext.buildContext();
    StreamSource<Bid> source = new StreamSource<>(streamingContext, new BidSourceFunction(srcRate));
    source
        .flatMap(
            new FlatMapFunction<Bid, Record4>() {
              @Override
              public void flatMap(Bid bid, Collector<Record4> collector) {
                if (bid.auction % 1007 == 0
                    || bid.auction % 1020 == 0
                    || bid.auction % 2001 == 0
                    || bid.auction % 2019 == 0
                    || bid.auction % 2087 == 0) {
                  collector.collect(new Record4(bid.auction, bid.price, bid.bidder, bid.dateTime));
                }
              }
            })
        .sink(new DummyLatencyCountingSink(logger));

    streamingContext.execute();
    try {
      Thread.sleep(1000000000);
    } catch (InterruptedException e) {
    }
  }
}
