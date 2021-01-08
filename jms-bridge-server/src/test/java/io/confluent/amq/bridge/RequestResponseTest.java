/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ForkJoinPool;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicRequestor;
import javax.jms.TopicSession;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@SuppressFBWarnings("MS_SHOULD_BE_FINAL")
public class RequestResponseTest {

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
          .mutateJmsBridgeConfig(br -> br
              .putKafka(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
              .putKafka(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
              .routing(new RoutingConfig.Builder()
                  .addTopics(new RoutedTopic.Builder()
                      .messageType("TEXT")
                      .match("response.*")
                      .consumeAlways(true)
                      .addressTemplate("test.${topic}"))
                  .addTopics(new RoutedTopic.Builder()
                      .messageType("TEXT")
                      .match("request.*")
                      .addressTemplate("test.${topic}"))
                  .build())
          )
          .dataDirectory(tempdir.toAbsolutePath().toString()));


  @Test
  public void testKafkaTopicAddressIsAvailable() throws Exception {
    String requestTopic = kafkaContainer.safeCreateTopic("request", 3);
    String responseTopic = kafkaContainer.safeCreateTopic("response", 3);

    amqServer.confluentAmqServer().getKafkaExchangeManager().synchronizeTopics();

    String requestAddress = "test." + requestTopic;
    amqServer.assertAddressAvailable(requestAddress);

    String responseAddress = "test." + responseTopic;
    amqServer.assertAddressAvailable(responseAddress);

    Properties responderProps = new Properties();
    responderProps.putAll(kafkaContainer.defaultProps());
    responderProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    responderProps.put(ConsumerConfig.GROUP_ID_CONFIG, amqServer.safeId("test-group"));

    Map<String, String> randrMap = new LinkedHashMap<>();
    randrMap.put("Hi, what's your name?", "My name is Kafka.");
    randrMap.put("Can you speak JMS?", "Not so much, but I have a friend who can.");
    randrMap.put("Oh, do you know JaMeS BRIDGE?", "Yep, he's the one.");
    randrMap.put("Very helpful having him around, later then.", "I agree, later.");

    KafkaResponder responder = new KafkaResponder(
        responderProps, randrMap, requestTopic, responseTopic);
    ForkJoinPool.commonPool().execute(responder);

    //wait for the thread to start consuming
    responder.awaitReady();

    try (
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic requestAmqTopic = session.createTopic(requestAddress);

      TopicSession topicSession = (TopicSession) session;
      TopicRequestor requestor = new TopicRequestor(topicSession, requestAmqTopic);

      randrMap.entrySet().forEach(en -> {
        try {
          TextMessage tmsg = session.createTextMessage(en.getKey());
          Message response = requestor.request(tmsg);
          assertNotNull(response);
          assertEquals(en.getValue(), response.getBody(String.class));
          System.out.println("response: " + response.getBody(String.class));
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } finally {
      System.out.println("jms disconnected.");
      //stop thread
      responder.stopAndAwaitCompletion();
    }
  }

  public static class KafkaResponder implements Runnable {

    private final Properties kafkaProps;
    private final Map<String, String> requestResponseMap;
    private final String requestTopic;
    private final String responseTopic;
    private final CountDownLatch readyLatch = new CountDownLatch(1);
    private final CountDownLatch stoppedLatch = new CountDownLatch(1);
    private volatile boolean running = false;

    public KafkaResponder(Properties kafkaProps,
        Map<String, String> requestResponseMap, String requestTopic, String responseTopic) {
      this.kafkaProps = kafkaProps;
      this.requestResponseMap = requestResponseMap;
      this.requestTopic = requestTopic;
      this.responseTopic = responseTopic;
    }

    public void awaitReady() {
      try {
        readyLatch.await();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void stopAndAwaitCompletion() {
      if (!running) {
        return;
      }

      running = false;
      try {
        stoppedLatch.await();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void run() {
      running = true;
      try (
          KafkaProducer<byte[], String> kafkaProducer =
              new KafkaProducer<>(kafkaProps, new ByteArraySerializer(), new StringSerializer());
          KafkaConsumer<byte[], String> kconsumer = new KafkaConsumer<>(
              kafkaProps, new ByteArrayDeserializer(), new StringDeserializer())
      ) {

        kconsumer.subscribe(Collections.singleton(requestTopic));
        while (running) {

          ConsumerRecords<byte[], String> pollRecords = kconsumer.poll(Duration.ofMillis(100L));
          if (pollRecords != null) {
            pollRecords.forEach(krecord -> {
              Header replyTo = krecord.headers().lastHeader("jms.JMSReplyTo");
              final byte[] destination = replyTo != null
                  ? replyTo.value()
                  : null;


              System.out.println("Request: " + krecord.value());

              final String response = requestResponseMap
                  .getOrDefault(krecord.value(), "NO RESPONSE FOUND");

              ProducerRecord<byte[], String> responseRecord = new ProducerRecord<>(
                  responseTopic, krecord.key(), response);
              if (destination != null) {
                responseRecord.headers().add("jms.JMSDestination", destination);
              }
              try {
                kafkaProducer.send(responseRecord).get();
              } catch (Exception e) {
                throw new RuntimeException(e);
              }
            });
            readyLatch.countDown();
          }
        }
        System.out.println("kafka disconnected");
      } catch (Exception e) {
        e.printStackTrace();
      } finally {
        stoppedLatch.countDown();
      }
    }
  }

}
