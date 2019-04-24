/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.ray.streaming.nexmark.sinks;

import org.ray.streaming.util.Record4;
import org.ray.streaming.api.function.impl.SinkFunction;

import org.slf4j.Logger;

/** A Sink that drops all data and periodically emits latency measurements */
public class DummyLatencyCountingSink implements SinkFunction<Record4> {

  private final Logger logger;

  public DummyLatencyCountingSink(Logger log) {
    logger = log;
  }

  @Override
  public void sink(Record4 record) {
    logger.info("bid: {} {} {} {}", record.getF0(), record.getF1(), record.getF2(), record.getF3());
  }
}
