/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import static io.confluent.amq.SerdePool.ser;

import com.google.common.base.Stopwatch;
import io.confluent.amq.persistence.kafka.KafkaIO;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.apache.kafka.common.serialization.StringDeserializer;
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


  public static KafkaTestContainer usingDefaults() {
    KafkaContainer container = new KafkaContainer("5.5.2")
        .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
        .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

    return new KafkaTestContainer(container);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaTestContainer.class);

  private final AtomicInteger sequence = new AtomicInteger(0);
  private final KafkaContainer kafkaContainer;
  private final Properties baseProps;
  private final List<String> tempTopics;

  private AdminClient adminClient;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaIO kafkaIO;

  public KafkaTestContainer(Properties baseProps, KafkaContainer kafkaContainer) {
    this.kafkaContainer = kafkaContainer;
    this.tempTopics = new LinkedList<>();
    this.baseProps = baseProps;
  }

  public KafkaTestContainer(KafkaContainer kafkaContainer) {
    this.kafkaContainer = kafkaContainer;
    this.tempTopics = new LinkedList<>();
    this.baseProps = new Properties();
  }

  @Override
  public void beforeAll(ExtensionContext extensionContext) throws Exception {
    this.kafkaContainer.start();


    Properties kafkaProps = defaultProps();
    kafkaProps.putAll(baseProps);
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-container");

    this.adminClient = AdminClient.create(kafkaProps);
    this.producer = new KafkaProducer<>(
        kafkaProps, new ByteArraySerializer(), new ByteArraySerializer());

    this.kafkaIO = new KafkaIO(kafkaProps);
    this.kafkaIO.start();
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
    this.kafkaIO.stop();
    this.kafkaContainer.stop();
  }

  public AdminClient adminClient() {
    return adminClient;
  }

  public KafkaIO getKafkaIO() {
    return this.kafkaIO;
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

  public List<ConsumerRecord<byte[], String>> consumeBytesStringsUntil(
      String topic,
      int expectedRecordCount) {

    return consumeUntil(
        topic,
        new ByteArrayDeserializer(),
        new StringDeserializer(),
        expectedRecordCount,
        Duration.ofSeconds(5));
  }


  public List<ConsumerRecord<byte[], byte[]>> consumeBytesUntil(
      String topic,
      int expectedRecordCount) {

    return consumeUntil(
        topic,
        new ByteArrayDeserializer(),
        new ByteArrayDeserializer(),
        expectedRecordCount,
        Duration.ofSeconds(5));
  }

  public List<ConsumerRecord<String, String>> consumeStringsUntil(
      String topic,
      int expectedRecordCount) {

    return consumeUntil(
        topic,
        new StringDeserializer(),
        new StringDeserializer(),
        expectedRecordCount,
        Duration.ofSeconds(5));
  }


  public <K, V> List<ConsumerRecord<K, V>> consumeAll(
      String topic,
      Deserializer<K> keydeser,
      Deserializer<V> valuedeser) {

    return consumeUntil(topic, keydeser, valuedeser, Integer.MAX_VALUE, Duration.ofSeconds(1));
  }

  public <K, V> List<ConsumerRecord<K, V>> consumeUntil(
      String topic,
      Deserializer<K> keydeser,
      Deserializer<V> valuedeser,
      int expectedRecordCount,
      Duration maxDuration) {

    Properties kafkaProps = defaultProps();
    kafkaProps.putAll(baseProps);
    kafkaProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-test-container");
    final Stopwatch totalTime = Stopwatch.createStarted();

    KafkaConsumer<K, V> consumer = new KafkaConsumer<>(kafkaProps, keydeser, valuedeser);

    List<TopicPartition> ptList = consumer.partitionsFor(topic).stream()
        .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
        .collect(Collectors.toList());

    if (!ptList.isEmpty()) {
      consumer.assign(ptList);
      consumer.seekToBeginning(ptList);
    }

    List<ConsumerRecord<K, V>> recordList = new LinkedList<>();
    Stopwatch stopwatch = Stopwatch.createStarted();
    while (recordList.size() < expectedRecordCount
        && stopwatch.elapsed().minus(maxDuration).isNegative()) {

      ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
      records.forEach(recordList::add);

    }

    consumer.close();
    System.out.println(
        "KafkaTestContainer.consumerUntil took "
        + totalTime.elapsed().toMillis() + "ms");
    return recordList;
  }

  public void createTopic(String name, int partitions) {
    createTopic(name, partitions, Collections.emptyMap());
  }

  public void createTopic(String name, int partitions, Map<String, String> options) {
    NewTopic topic = new NewTopic(name, partitions, (short) 1);
    topic.configs(options);

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

  public String safeCreateTopic(String prefix, int partitions) {
    String name = String.format("%s-%d", prefix, sequence.getAndIncrement());
    createTopic(name, partitions);
    return name;
  }

  public String safeTempTopic(String prefix, int partitions) {
    String name = String.format("%s-%d", prefix, sequence.getAndIncrement());
    createTempTopic(name, partitions);
    return name;
  }

  public void createTempTopic(String name, int partitions) {
    createTempTopic(name, partitions, Collections.emptyMap());
  }

  public void createTempTopic(String name, int partitions, Map<String, String> options) {
    this.createTopic(name, partitions, options);
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
        Set<String> knownTopics = listTopics();
        if (topiclist.stream().anyMatch(knownTopics::contains)) {
          Thread.sleep(50);
        } else {
          break;
        }
      }
    } catch (Exception e) {
      System.out.println("Exception occurred while deleting topicList " + topiclist);
      e.printStackTrace();
    }
  }

  public Set<String> listTopics() {
    try {
      return this.adminClient.listTopics().names().get();
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
    deleteTopics(listTopics());
  }

  public Properties defaultProps() {
    Properties kafkaProps = new Properties();
    kafkaProps.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
    return kafkaProps;
  }
}
