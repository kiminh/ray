package org.ray.streaming.nexmark;

import org.kohsuke.args4j.Option;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.nexmark.sinks.DummyLatencyCountingSink;
import org.ray.streaming.nexmark.sources.BidSourceFunction;
import org.apache.beam.sdk.nexmark.model.Bid;
import org.ray.streaming.util.Record4;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.Serializable;

public class Query1 implements Serializable {

  private static final Logger logger = LoggerFactory.getLogger(Query1.class);

  @Option(name = "--exchange-rate", usage = "exchange-rate")
  private float exchangeRate = 0.82F;

  @Option(name = "--src-rate", usage = "srcRate")
  private int srcRate = 100000;

  public static void main(String[] args) throws Exception {
    new Query1().doMain(args);
  }

  public void doMain(String[] args) throws Exception {

    StreamingContext streamingContext = StreamingContext.buildContext();
    StreamSource<Bid> source = new StreamSource<>(streamingContext, new BidSourceFunction(srcRate));
    source.map(bid -> new Record4(bid.auction, dollarToEuro(bid.price, exchangeRate), bid.bidder, bid.dateTime))
        .sink(new DummyLatencyCountingSink(logger));

    streamingContext.execute();
    try {
      Thread.sleep(1000000000);
    } catch (InterruptedException e) {
    }
  }

  private static long dollarToEuro(long dollarPrice, float rate) {
    return (long) (rate * dollarPrice);
  }
}
