/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.clients;

import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;

public final class CommonMetrics {

  private static double[] BUCKETS = {
      1, 5, 10, 25, 50, 100, 250, 500, 750, 1000, 1500, 2000, 3000, 4000, 5000
  };

  public static final Histogram MESSAGE_LATENCY_HIST = Histogram.build()
      .buckets(BUCKETS)
      .name("perf_test_producer_to_consumer_latency_ms")
      .help("Time it takes for message to be consumed from the producer")
      .register();



  public static final Gauge MSG_CONSUMED_SIZE_GAUGE = Gauge.build()
      .name("perf_test_message_size")
      .help("The size of the message being received")
      .register();

  public static final Counter MSG_CONSUMED_COUNT = Counter.build()
      .name("perf_test_messages_consumed")
      .help("The number of messages produced.")
      .register();

  public static final Counter MSG_CONSUMED_ERROR_COUNT = Counter.build()
      .name("perf_test_message_errors_consumed")
      .help("The number of message errors encountered.")
      .register();



  public static final Counter MSG_PRODUCER_COUNT = Counter.build()
      .name("perf_test_messages_produced")
      .help("The number of messages produced.")
      .register();

  public static final Counter MSG_PRODUCER_ERROR_COUNT = Counter.build()
      .name("perf_test_message_errors_produced")
      .help("The number of message errors encountered.")
      .register();

  private CommonMetrics() {
  }

}
