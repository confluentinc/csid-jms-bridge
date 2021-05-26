/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.mq.perf.data;

import com.google.common.primitives.Bytes;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class DataGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);
  private static final AtomicInteger IDX = new AtomicInteger();

  private static final Counter GEN_COUNT = Counter.build()
      .name("perf_test_messages_generated")
      .help("The number of messages generated for testing.")
      .register();

  private static final Gauge GEN_BUFFER_DEPTH = Gauge.build()
      .name("perf_test_messages_generated_buffer_depth")
      .help("How full the generated message buffer is.")
      .register();

  private static final String BASE_STR =
      "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ !@#?><:;][{}1234567890";

  private final Random rand;
  private final int preBufferSize;
  private final int messagePartSize;
  private final int messagePartSamples;
  private final int partsPerMessage;
  private final int remainderSamples;
  private final int remainderSize;

  public DataGenerator(int messageSize, Random rand) {
    this.rand = rand;
    this.preBufferSize = 100;

    this.remainderSize = messageSize % 100;
    this.remainderSamples = 1000;

    this.messagePartSize = 100;
    this.partsPerMessage = messageSize / messagePartSize;
    this.messagePartSamples = (0 == this.partsPerMessage ? 0 : 100);
  }

  public DataGenerator(int messageSize) {
    this(messageSize, new Random());
  }

  public Supplier<byte[]> createMessageSupplier() {

    byte[] baseBytes = BASE_STR.getBytes(StandardCharsets.UTF_8);

    List<byte[]> samples = createSamples(messagePartSize, messagePartSamples, baseBytes, rand);
    List<byte[]> remSamples = createSamples(remainderSize, remainderSamples, baseBytes, rand);
    final BlockingQueue<byte[]> messageQueue = new LinkedBlockingQueue<>(preBufferSize);

    Thread sampleThread = new Thread(() -> {
      while (true) {
        byte[][] chosenParts = new byte[partsPerMessage + 1][];
        for (int i = 0; i < partsPerMessage; i++) {
          chosenParts[i] = samples.get(rand.nextInt(samples.size()));
        }
        //add remainder
        chosenParts[partsPerMessage] = remSamples.get(rand.nextInt(remSamples.size()));

        try {
          if (messageQueue.offer(Bytes.concat(chosenParts), 60, TimeUnit.SECONDS)) {
            GEN_COUNT.inc();
            GEN_BUFFER_DEPTH.inc();
          }
        } catch (InterruptedException e) {
          LOGGER.info("Message generator stopping");
          break;
        }
      }
    });
    sampleThread.setDaemon(true);
    sampleThread.setName("sample-message-generator-" + IDX.getAndIncrement());
    sampleThread.start();

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      sampleThread.interrupt();
    }));

    return () -> {
      try {
        byte[] msg = messageQueue.poll(1, TimeUnit.SECONDS);
        GEN_BUFFER_DEPTH.dec();
        return msg;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  private List<byte[]> createSamples(
      int sampleSize, int sampleCount, byte[] baseData, Random rand) {

    List<byte[]> samples = new ArrayList<>(sampleCount);
    for (int i = 0; i < sampleCount; i++) {
      byte[] byteSample = new byte[sampleSize];
      for (int j = 0; j < sampleSize; j++) {
        byteSample[j] = baseData[rand.nextInt(baseData.length)];
      }
      samples.add(byteSample);
    }
    return samples;
  }
}
