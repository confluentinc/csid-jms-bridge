/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import javax.annotation.Nullable;

import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import io.confluent.amq.persistence.kafka.journal.serde.JournalKeySerde;
import io.confluent.amq.persistence.kafka.journal.serde.JournalValueSerde;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaIO {

  private static final int CREATED = 0;
  private static final int STARTED = 1;
  private static final int STOPPED = 2;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIO.class);
  private static final LogFormat LOG_FORMAT = LogFormat.forSubject("KafkaIO");
  private static final AtomicInteger ID_SEQ = new AtomicInteger(0);

  private final Properties kafkaProps;
  private final BridgeClientId baseClientId;


  private final ReadWriteLock rwlock = new ReentrantReadWriteLock();
  private volatile int state;
  private volatile KafkaProducer<JournalEntryKey, JournalEntry> internalProducer;
  private volatile KafkaProducer<byte[], byte[]> externalProducer;
  private volatile AdminClient adminClient;

  public KafkaIO(BridgeClientId baseClientId, Map<?, ?> kafkaProps) {
    this.baseClientId = baseClientId;
    this.kafkaProps = new Properties();
    this.kafkaProps.putAll(kafkaProps);
    this.kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    this.kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");
    this.kafkaProps.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG,
        JournalEntryKeyPartitioner.class.getCanonicalName());

  }

  protected void start() {
    rwlock.writeLock().lock();
    try {
      if (CREATED == state || STOPPED == state) {
        Serializer<JournalEntry> valueSerializer = JournalValueSerde.DEFAULT.serializer();
        Serializer<JournalEntryKey> keySerializer = JournalKeySerde.DEFAULT.serializer();
        internalProducer = createProducer("internal", keySerializer, valueSerializer);
        externalProducer = createProducer("external",
            new ByteArraySerializer(), new ByteArraySerializer());

        Properties adminProps = new Properties();
        adminProps.putAll(kafkaProps);
        adminProps.put(
                AdminClientConfig.CLIENT_ID_CONFIG,
                baseClientId.clientId("admin-" + ID_SEQ.getAndIncrement()));
        adminClient = AdminClient.create(adminProps);
        Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        state = STARTED;
      }
    } finally {
      rwlock.writeLock().unlock();
    }
  }

  public ConsumerGroupDescription describeConsumerGroup(String consumerGroup) {
    return fetchWithAdminClient(admin -> {
      try {
        Map<String, ConsumerGroupDescription> results = admin
            .describeConsumerGroups(Collections.singletonList(consumerGroup)).all().get();
        return results.get(consumerGroup);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }


  /**
   * Creates a new consumer thread and starts it. It does not manage any more of it's lifecycle past
   * that point and is up to the caller to ensure it is stopped properly.
   */
  public <K, V> ConsumerThread<K, V> startConsumerThread(
      Consumer<? super ConsumerThread.Builder<K, V>> spec) {
    ConsumerThread.Builder<K, V> builder = ConsumerThread.newBuilder();
    kafkaProps.forEach((k, v) -> builder.putConsumerProps(k.toString(), v));
    builder.putConsumerProps(
            ConsumerConfig.CLIENT_ID_CONFIG, baseClientId.clientId("admin-" + ID_SEQ.getAndIncrement()));
    spec.accept(builder);

    ConsumerThread<K, V> consumerThread = builder.build();

    Thread thread = new Thread(consumerThread,
        "kafka-consumer-" + consumerThread.groupId());
    thread.setUncaughtExceptionHandler(consumerThread.exceptionHandler());
    thread.setDaemon(true);

    thread.start();
    return consumerThread;
  }

  public Set<String> listTopics() {
    return fetchWithAdminClient(KafkaIO::listTopics);
  }

  private static Set<String> listTopics(Admin admin) {
    try {
      return admin.listTopics().names().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Map<String, TopicDescription> describeTopics(Collection<String> topics) {
    return fetchWithAdminClient(admin -> {
      try {
        return admin.describeTopics(topics).all().get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }


  public void createTopic(String name, int partitions, int replication,
      Map<String, String> options) {

    createTopic(name, partitions, replication, false, options);
  }

  /**
   * Returns true if it did create the topic false if it did not because it already exists and the
   * checkIfExists option was true.
   *
   * @param name          the name of the topic
   * @param partitions    the desired number of partitions
   * @param replication   the desired replication factor
   * @param checkIfExists check if the topic exists before creating it
   * @param options       any other topic options
   */
  private void createTopic(
      String name,
      int partitions,
      int replication,
      boolean checkIfExists,
      Map<String, String> options) {

    fetchWithAdminClient(admin -> {
      LOGGER.info(LOG_FORMAT.build(b -> b
          .event("CreateTopic")
          .putTokens("topic", name)
          .putTokens("partitions", partitions)
          .putTokens("replication", replication)));

      if (checkIfExists) {
        if (listTopics(admin).contains(name)) {
          return false;
        }
      }

      NewTopic topic = new NewTopic(name, partitions, (short) replication);
      topic.configs(options);
      try {
        admin.createTopics(Collections.singletonList(topic)).all().get();
      } catch (ExecutionException | InterruptedException e) {
        if (e.getCause() instanceof TopicExistsException) {
          LOGGER.info(LOG_FORMAT.build(b -> b
              .event("CreateTopic")
              .markFailure()
              .message("Topic already exists, moving on.")
              .putTokens("topic", name)));
        }
      }

      return true;
    });
  }

  /**
   * <p>
   * The internal producer is used to publish data to topics used internally by the JMS bridge and
   * not meant for consumption outisde of it.
   *</p>
   *<p>
   * If {@link #start()} has not been called prior to this then null may be returned.
   *</p>
   * @return the internal producer, may be null if this instance has not been started.
   */
  @Nullable
  public  KafkaProducer<JournalEntryKey, JournalEntry> getInternalProducer() {
    return internalProducer;
  }

  /**
   * <p>
   * The external producer is used to publish data to topics used not managed by the JMS Bridge.
   * These are topics that may be consumed by other applications.
   *</p>
   *<p>
   * If {@link #start()} has not been called prior to this then null may be returned.
   *</p>
   * @return the external producer, may be null if this instance has not been started.
   */
  @Nullable
  public KafkaProducer<byte[], byte[]> getExternalProducer() {
    return externalProducer;
  }

  @SuppressWarnings("unchecked")
  public void withProducer(
      Consumer<? super Producer<JournalEntryKey, JournalEntry>> produceFn) {
    if (STARTED != state) {
      start();
    }

    rwlock.readLock().lock();
    try {
      produceFn.accept(internalProducer);
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public <K, V> KafkaProducer<K, V> createProducer(
      String clientSuffix, Serializer<K> keySer, Serializer<V> valSer) {

    Map<String, Object> lprops = new HashMap<>();
    lprops.putAll(BridgeConfigFactory.propsToMap(kafkaProps));
    lprops.put(
        ProducerConfig.CLIENT_ID_CONFIG,
        baseClientId.clientId(String.format("%s_%s", clientSuffix, ID_SEQ.getAndIncrement())));
    KafkaProducer<K, V> producer = new KafkaProducer<>(lprops, keySer, valSer);
    Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    return producer;
  }

  public <T> T fetchWithAdminClient(Function<? super AdminClient, T> adminClientFn) {
    if (STARTED != state) {
      start();
    }

    rwlock.readLock().lock();
    try {
      return adminClientFn.apply(adminClient);

    } finally {
      rwlock.readLock().unlock();
    }
  }

  public void stop() {
    rwlock.writeLock().lock();
    try {
      if (STARTED == state) {
        internalProducer.close();
        adminClient.close();
      }
      state = STOPPED;
    } finally {
      rwlock.writeLock().unlock();
    }
  }
}
