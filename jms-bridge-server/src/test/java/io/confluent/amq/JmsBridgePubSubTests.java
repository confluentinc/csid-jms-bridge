/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import com.google.common.base.Stopwatch;
import com.google.common.base.Strings;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.micrometer.core.instrument.Timer;
import javax.jms.CompletionListener;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.artemis.core.server.metrics.MetricsManager;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.confluent.amq.test.TestSupport.LOGGER;
import static io.confluent.amq.test.TestSupport.println;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JmsBridgePubSubTests {

  private static final boolean IS_VANILLA = false;
  private static final String JMS_TOPIC = "jms-to-kafka";

  @TempDir
  @Order(100)
  public static Path tempdir;

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      new KafkaContainer("5.5.2")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false"));

  @RegisterExtension
  @Order(300)
  public static final ArtemisTestServer amqServer = ArtemisTestServer
      .embedded(kafkaContainer, b -> b
          .useVanilla(IS_VANILLA)
          .mutateJmsBridgeConfig(br -> br
              .putStreams(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500"))
          .dataDirectory(tempdir.toAbsolutePath().toString()));

  @Disabled("Manual local load test")
  @Test
  public void jms2jmsVolume() throws Exception {
    String topicName = amqServer.safeId("/test/perf/jms2jmsVolume");
    String subscriberName = amqServer.safeId("jms2jmsVolume-subscriber");

    try (
        Session session1 = amqServer.getConnection()
            .createSession(false, Session.CLIENT_ACKNOWLEDGE);
        Session session2 = amqServer.getConnection()
            .createSession(false, Session.CLIENT_ACKNOWLEDGE);
    ) {

      Topic topic = session1.createTopic(topicName);
      final Semaphore sync = new Semaphore(0);
      final String message = Strings.repeat("foobar foo", 11);
      final int samples = 1_000_000;
      MetricsManager metricsManager = amqServer.metricsManager();

      final Timer producerTimer = Timer.builder("test-perf-producer")
          .maximumExpectedValue(Duration.ofSeconds(1))
          .publishPercentiles(0.75, 0.9, 0.95)
          .register(metricsManager.getMeterRegistry());

      final Timer consumerTimer = Timer.builder("test-perf-consumer")
          .maximumExpectedValue(Duration.ofSeconds(1))
          .publishPercentiles(0.75, 0.9, 0.95)
          .register(metricsManager.getMeterRegistry());

      amqServer.confluentAmqServer().getScheduledPool()
          .scheduleAtFixedRate(() -> {

            LOGGER.info("METRICS");

            Stream.of(producerTimer.takeSnapshot().percentileValues()).forEach(p ->
                LOGGER.info("Producer {} %ile: {} ms, {} mps",
                    p.percentile(),
                    p.value(TimeUnit.MILLISECONDS),
                    1000 / p.value(TimeUnit.MILLISECONDS)));
            Stream.of(consumerTimer.takeSnapshot().percentileValues()).forEach(p ->
                LOGGER.info("Consumer {} %ile: {} ms, {} mps",
                    p.percentile(),
                    p.value(TimeUnit.MILLISECONDS),
                    1000 / p.value(TimeUnit.MILLISECONDS)));

          }, 5, 5, TimeUnit.SECONDS);

      CompletableFuture<?> consumerFuture = CompletableFuture.runAsync(() -> {
        try (
            MessageConsumer consumer = session1.createDurableConsumer(topic, subscriberName)
        ) {
          boolean keepGoing = true;
          int count = 0;
          try {
            consumer.setMessageListener(new MessageListener() {
              final Stopwatch stopwatch = Stopwatch.createStarted();
              int count = 0;

              @Override
              public void onMessage(Message message) {
                if (message != null) {
                  stopwatch.stop();
                  consumerTimer.record(stopwatch.elapsed());
                  count++;
                  if (count % 100 == 0) {
                    try {
                      message.acknowledge();
                    } catch (Exception e) {
                      LOGGER.error("Failed to ack message", e);
                    }
                  }
                  stopwatch.reset().start();
                }
              }
            });
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
          while (keepGoing) {
            keepGoing = !sync.tryAcquire(1, 30, TimeUnit.SECONDS);
          }
          consumer.setMessageListener(null);
          LOGGER.info("Consumer completed");
        } catch (Exception e) {
          LOGGER.error("Consumer failed with exception", e);
        }
      });

      CompletableFuture<?> producerFuture = CompletableFuture.runAsync(() -> {
        try (
            MessageProducer producer = session2.createProducer(topic);
        ) {
          try {
            Thread.sleep(1000);
          } catch (Exception e) {
            throw new RuntimeException(e);
          }

          for (int count = 0; count < samples; count++) {
            producer.send(session2.createTextMessage(message), new TimedListener(producerTimer));
          }
          sync.release();
          LOGGER.info("Producer completed");
        } catch (Exception e) {
          LOGGER.error("Producer failed with exception", e);
        }
      });

      LOGGER.info("Waiting for producer and consumer to finish.");
      CompletableFuture.allOf(producerFuture, consumerFuture).join();
    }
  }

  @Test
  @Timeout(30)
  public void jmsClientPubSubMultipleBindings() throws Exception {
    String topicName = amqServer.safeId("jms-client-multi-bindings");
    String subscriberName = amqServer.safeId("jms-client-multi-bindings-subscriber");

    try (
        Session session1 = amqServer.getConnection()
            .createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session session2 = amqServer.getConnection()
            .createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic topic = session1.createTopic(topicName);

      try (
          MessageProducer producer = session1.createProducer(topic);
          MessageConsumer consumer1 = session1
              .createDurableConsumer(topic, subscriberName + "-1");
          MessageConsumer consumer2 = session2
              .createDurableConsumer(topic, subscriberName + "-2")
      ) {

        producer.send(session1.createTextMessage("Message 1"));
        Message rcvmsg1 = consumer1.receive(100);
        Message rcvmsg2 = consumer2.receive(100);

        assertEquals("Message 1", rcvmsg1.getBody(String.class));
        assertEquals("Message 1", rcvmsg2.getBody(String.class));
      }
    }
  }

  @Test
  @Timeout(30)
  public void jmsBasicPubSub() throws Exception {
    Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    Topic topic = session.createTopic(amqServer.safeId(JMS_TOPIC));
    MessageProducer producer = session.createProducer(topic);

    //without a consumer the message isn't routable so it will never be stored
    //It also must be targetting a durable queue
    MessageConsumer consumer = session
        .createDurableConsumer(topic, amqServer.safeId("test-subscriber"));

    //allow the consumer to get situated.
    Thread.sleep(1000);

    int count = 100;
    try {
      for (int i = 0; i < count; i++) {
        producer.send(session.createTextMessage("Hello JMS Bridge " + i));
      }

      for (int i = 0; i < count; i++) {
        Message received = consumer.receive(100);
        assertNotNull(received);
        assertEquals("Hello JMS Bridge " + i, received.getBody(String.class));
      }

    } finally {
      println("Closing JMS session and connection");
      consumer.close();
      session.close();
    }

    if (!IS_VANILLA) {
      String bindingsJournal = amqServer.bindingsJournalTableTopic();
      logJournalFiles(bindingsJournal);

      String messagesJournal = amqServer.messageJournalTableTopic();
      logJournalFiles(messagesJournal);
    }
  }

  public Stream<Pair<JournalEntryKey, JournalEntry>> streamJournalFiles(String journalTopic) {
    return kafkaContainer
        .consumeAll(journalTopic, new ByteArrayDeserializer(), new ByteArrayDeserializer())
        .stream()
        .map(r -> {

          JournalEntryKey rkey = null;
          if (r.key() != null) {
            try {
              rkey = JournalEntryKey.parseFrom(r.key());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          JournalEntry rval = null;
          if (r.value() != null) {
            try {
              rval = JournalEntry.parseFrom(r.value());
            } catch (Exception e) {
              throw new RuntimeException(e);
            }
          }

          return Pair.of(rkey, rval);
        });
  }

  public void logJournalFiles(String journalTopic) {
    LogFormat format = LogFormat.forSubject("JournalLog");

    String journalStr = streamJournalFiles(journalTopic)
        .map(pair -> format.build(b -> {

          b.addJournalEntryKey(pair.getKey());
          if (pair.getValue() == null) {
            b.event("TOMBSTONE");
          } else {
            b.event("ENTRY");
            b.addJournalEntry(pair.getValue());
          }

        }))
        .collect(Collectors.joining(System.lineSeparator()));

    println(
        "#### JOURNAL FOR TOPIC " + journalTopic + " ####" + System.lineSeparator()
            + journalStr);
  }

  public static class TimedListener implements CompletionListener {

    final Timer timer;
    final Stopwatch stopwatch = Stopwatch.createStarted();

    public TimedListener(Timer timer) {
      this.timer = timer;
    }

    @Override
    public void onCompletion(Message message) {
      stopwatch.stop();
      timer.record(stopwatch.elapsed());
    }

    @Override
    public void onException(Message message, Exception exception) {
      LOGGER.error("Failed to publish message", exception);
    }
  }
}
