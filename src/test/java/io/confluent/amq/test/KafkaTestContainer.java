/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import static io.confluent.amq.SerdePool.ser;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaTestContainer implements
    BeforeAllCallback, AfterAllCallback, BeforeEachCallback, AfterEachCallback {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestContainer.class);

  private final KafkaContainer kafkaContainer;
  private final List<String> tempTopics;

  private AdminClient adminClient;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaConsumer<byte[], byte[]> consumer;

  public KafkaTestContainer(KafkaContainer kafkaContainer) {
    this.kafkaContainer = kafkaContainer;
    this.tempTopics = new LinkedList<>();
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    this.kafkaContainer.start();

    Properties kafkaProps = defaultProps();
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-container");

    this.adminClient = AdminClient.create(kafkaProps);
    this.producer = new KafkaProducer<>(
        kafkaProps, new ByteArraySerializer(), new ByteArraySerializer());
    this.consumer = new KafkaConsumer<>(
        kafkaProps, new ByteArrayDeserializer(), new ByteArrayDeserializer());
  }

  @Override
  public void beforeEach(ExtensionContext extensionContext) throws Exception {

  }

  @Override
  public void afterEach(ExtensionContext extensionContext) throws Exception {
    deleteTempTopics();
  }

  @Override
  public void afterAll(ExtensionContext extensionContext) throws Exception {
    this.adminClient.close();
    this.producer.close();
    this.consumer.close();
    this.kafkaContainer.stop();
  }


  public KafkaContainer getKafkaContainer() {
    return kafkaContainer;
  }

  public RecordMetadata publish(String topic, String key, String value) {
    return publish(topic, ser(topic, key), ser(topic, value));
  }

  public RecordMetadata publish(String topic, byte[] key, byte[] value) {
    try {
      return producer.send(new ProducerRecord<>(topic, key, value)).get();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public <K, V> List<ConsumerRecord<K, V>> consumeAll(String topic, Deserializer<K> keydeser,
      Deserializer<V> valuedeser) {

    List<TopicPartition> ptList = consumer.partitionsFor(topic).stream()
        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
        .collect(Collectors.toList());

    if (!ptList.isEmpty()) {
      consumer.assign(ptList);
      consumer.seekToBeginning(ptList);
    }

    List<ConsumerRecord<K, V>> recordList = new LinkedList<>();
    while (true) {
      ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(100));
      if (records.count() < 1) {
        break;
      }
      records.forEach(r -> {
        ConsumerRecord<K, V> rnew = new ConsumerRecord<K, V>(
            r.topic(), r.partition(), r.offset(), r.timestamp(), r.timestampType(), 0L,
            r.serializedKeySize(), r.serializedValueSize(),
            keydeser.deserialize(r.topic(), r.key()), valuedeser.deserialize(r.topic(), r.value()),
            r.headers(), r.leaderEpoch());
        recordList.add(rnew);
      });
    }

    return recordList;
  }

  public void createTopic(String name, int partitions) {
    NewTopic topic = new NewTopic(name, partitions, (short) 1);

    try {
      adminClient.createTopics(Collections.singletonList(topic)).all().get();
    } catch (Exception e) {
      if (e.getCause() != null && e.getCause() instanceof TopicExistsException) {
        LOGGER.debug("Topic '{}' already exists", name);
      } else {
        throw new RuntimeException(e);
      }
    }
  }

  public void createTempTopic(String name, int partitions) {
    this.createTopic(name, partitions);
    tempTopics.add(name);
  }

  public void deleteTopics(String name, String... names) {
    List<String> topiclist = new LinkedList<>();
    topiclist.add(name);
    if (names != null) {
      topiclist.addAll(Arrays.asList(names));
    }
    deleteTopics(topiclist);
  }

  public void deleteTopics(Collection<String> topiclist) {

    try {
      adminClient.deleteTopics(topiclist).all().get();

      while (true) {
        Set<String> knownTopics = adminClient.listTopics().names().get();
        if (topiclist.stream().anyMatch(knownTopics::contains)) {
          Thread.sleep(50);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void deleteTempTopics() {
    if (!tempTopics.isEmpty()) {
      this.deleteTopics(tempTopics);
      tempTopics.clear();
    }
  }

  public void deleteAllTopics() {
    try {
      Set<String> knownTopics = adminClient.listTopics().names().get();
      deleteTopics(knownTopics);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public Properties defaultProps() {
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    return kafkaProps;
  }
}
