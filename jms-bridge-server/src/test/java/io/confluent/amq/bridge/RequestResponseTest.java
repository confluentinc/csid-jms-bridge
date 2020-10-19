/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.bridge;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicRequestor;
import javax.jms.TopicSession;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;

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

    Map<String, String> randrMap = new LinkedHashMap<>();
    randrMap.put("Hi, what's your name?", "My name is Kafka.");
    randrMap.put("Can you speak JMS?", "Not so much, but I have a friend who can.");
    randrMap.put("Oh, do you know JaMeS BRIDGE?", "Yep, he's the one.");
    randrMap.put("Very helpful having him around, later then.", "I agree, later.");

    Semaphore threadStages = responseThread(randrMap, requestTopic, responseTopic);
    //wait for the thread to start consuming
    threadStages.acquire(2);

    try (
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      Topic requestAmqTopic = session.createTopic(requestAddress);

      TopicSession topicSession = (TopicSession) session;
      TopicRequestor requestor = new TopicRequestor(topicSession, requestAmqTopic);

      randrMap.entrySet().forEach(en -> {
        try {
          TextMessage tmsg = session.createTextMessage(en.getKey());
          System.out.println("jms says: " + en.getKey());
          Message response = requestor.request(tmsg);
          assertNotNull(response);
          assertEquals(en.getValue(), response.getBody(String.class));
        } catch (RuntimeException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      });
    } finally {
      System.out.println("jms disconnected.");
      //stop thread
      threadStages.drainPermits();

      //wait for it to complete shutdown
      threadStages.acquire();
    }
  }

  public Semaphore responseThread(
      Map<String, String> requestResponseMap, String requestTopic, String responseTopic) {

    final Semaphore stages = new Semaphore(0);
    Runnable consumerThread = () -> {
      Properties kprops = new Properties();
      kprops.putAll(kafkaContainer.defaultProps());
      kprops.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
      kprops.put(ConsumerConfig.GROUP_ID_CONFIG, amqServer.safeId("test-group"));

      try (KafkaConsumer<byte[], String> kconsumer = new KafkaConsumer<>(
          kprops, new ByteArrayDeserializer(), new StringDeserializer())) {

        kconsumer.subscribe(Collections.singleton(requestTopic));
        Runnable poller = () -> {

          ConsumerRecords<byte[], String> pollRecords = kconsumer.poll(Duration.ofMillis(100L));
          if (pollRecords != null) {
            pollRecords.forEach(krecord -> {
              Header replyTo = krecord.headers().lastHeader("jms.JMSReplyTo");
              final byte[] destination = replyTo != null
                  ? replyTo.value()
                  : null;
              String response = requestResponseMap
                  .getOrDefault(krecord.value(), "NO RESPONSE FOUND");

              System.out.println("kafka says: " + response);
              kafkaContainer.publish(
                  responseTopic,
                  krecord.key(),
                  response.getBytes(),
                  rec -> {
                    if (destination != null) {
                      rec.headers().add("jms.JMSDestination", destination);
                    }
                  });
            });
          }
        };
        stages.release(1);
        poller.run();
        stages.release(3);
        while (stages.availablePermits() > 0) {
          poller.run();
        }
        System.out.println("kafka disconnected");
      } catch (Exception e) {
        e.printStackTrace();
      }
      stages.release();
    };

    ForkJoinPool.commonPool().execute(consumerThread);
    return stages;
  }

}
