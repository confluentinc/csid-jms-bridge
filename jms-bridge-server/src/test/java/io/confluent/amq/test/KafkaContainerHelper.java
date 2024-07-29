/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.test;

import com.google.common.base.Stopwatch;
import io.confluent.amq.persistence.kafka.KafkaIO;
import org.apache.commons.lang3.RandomUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.LinkedTransferQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TransferQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaContainerHelper {
  public static final String CLIENT_ID_PREFIX = "unit-test";
  private static final AtomicInteger CLIENT_ID_COUNTER = new AtomicInteger();
  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaContainerHelper.class);

  private final AtomicInteger sequence;
  private final KafkaContainer kafkaContainer;
  private final Properties baseProps;
  private final List<String> tempTopics;
  private final List<Closeable> clients = new LinkedList<>();

  private AdminClient adminClient;
  private AdminHelper adminHelper;
  private KafkaProducer<byte[], byte[]> producer;
  private KafkaIO kafkaIO;
  private boolean initComplete = false;

  public KafkaContainerHelper(
      KafkaContainer kafkaContainer) {

    this(kafkaContainer, null, new AtomicInteger(0));
  }

  public KafkaContainerHelper(
      KafkaContainer kafkaContainer,
      AtomicInteger sequence) {

    this(kafkaContainer, null, sequence);
  }

  public KafkaContainerHelper(
      KafkaContainer kafkaContainer,
      AdminClient adminClient,
      AtomicInteger sequence) {
    this.kafkaContainer = kafkaContainer;
    this.adminClient = adminClient;
    this.sequence = sequence;
    this.tempTopics = new LinkedList<>();
    this.baseProps = new Properties();

    kafkaContainer.start();
  }

  private Properties addClientId(String suffix) {
    Properties extProps = new Properties();
    extProps.putAll(this.baseProps);
    String clientId =  CLIENT_ID_PREFIX + "-" + suffix + "-" + CLIENT_ID_COUNTER.getAndIncrement();
    extProps.put(AdminClientConfig.CLIENT_ID_CONFIG, clientId);
    return extProps;
  }

  private synchronized void init() {
    if (!initComplete) {
      this.addBaseProp(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers())
          .addBaseProp(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
          .addBaseProp(ConsumerConfig.GROUP_ID_CONFIG,
              "kafka-test-container-" + sequence.getAndIncrement())
          .addBaseProp(ConsumerConfig.EXCLUDE_INTERNAL_TOPICS_CONFIG, "false");

      if (adminClient == null) {
        AdminClient client = AdminClient.create(addClientId("admin"));
        Runtime.getRuntime().addShutdownHook(new Thread(client::close));
        this.adminClient = client;
      }

      adminHelper = new AdminHelper(this.adminClient, sequence);
      initComplete = true;
    }
  }

  public KafkaContainerHelper addBaseProp(String propName, String propVal) {
    baseProps.setProperty(propName, propVal);
    return this;
  }

  public String bootstrapServers() {
    return kafkaContainer.getBootstrapServers();
  }

  public AdminHelper adminHelper() {
    init();
    return adminHelper;
  }

  public <K, V> ProducerHelper<K, V> producerHelper(Serializer<K> keySer, Serializer<V> valSer) {

    init();
    KafkaProducer<K, V> producer = new KafkaProducer<>(addClientId("producer"), keySer, valSer);
    ProducerHelper<K, V> helper = new ProducerHelper<>(producer);
    clients.add(helper);
    return helper;
  }

  public <K, V> ConsumerHelper<K, V> consumerHelper(
      Deserializer<K> keyDeser, Deserializer<V> valDeser) {

    init();
    KafkaConsumer<K, V> kafkaConsumer = new KafkaConsumer<>(addClientId("consumer"), keyDeser, valDeser);
    ConsumerHelper<K, V> helper = new ConsumerHelper<>(kafkaConsumer);
    clients.add(helper);
    return helper;
  }

  public void cleanUpTopics() {
    adminHelper().deleteTempTopics();
  }

  public void closeClients() {
    clients.parallelStream().forEach(c -> {
      try {
        c.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static final class ConsumerHelper<K, V> implements Closeable {
    private final KafkaConsumer<K, V> consumer;
    private final AtomicBoolean greenLight = new AtomicBoolean(true);
    private final TransferQueue<ConsumerRecord<K, V>> receivedRecords = new LinkedTransferQueue<>();
    private final Semaphore closed = new Semaphore(1);
    private final AtomicLong lag = new AtomicLong(-1);

    ConsumerHelper(KafkaConsumer<K, V> consumer) {
      this.consumer = consumer;
    }

    public void consumeBegin(String... topics) {
      consumerRunner(Arrays.asList(topics));
    }

    public List<ConsumerRecord<K, V>> consumeAll(String... topics) {
      consumeBegin(topics);
      List<ConsumerRecord<K, V>> results = new LinkedList<>();
      while (lag.get() != 0) {
        results.addAll(drainRecords());
      }
      close();
      return results;
    }

    private void consumerRunner(Collection<String> topics) {
      ForkJoinPool.commonPool().execute(() -> {
        try {
          closed.acquire();
          consumer.assign(topics.stream()
              .flatMap(t -> consumer.partitionsFor(t).stream())
              .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
              .collect(Collectors.toList()));
          consumer.seekToBeginning(consumer.assignment());
          Map<TopicPartition, Long> endOffsets = consumer.endOffsets(consumer.assignment());
          Map<TopicPartition, Long> lastOffset = new HashMap<>();

          while (greenLight.get()) {
            ConsumerRecords<K, V> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(cr -> {
              lastOffset.put(new TopicPartition(cr.topic(), cr.partition()), cr.offset());
              receivedRecords.add(cr);
              LOGGER.info("Found record with payload: {}",cr.value());
            });
            lag.set(endOffsets.entrySet()
                .stream()
                .mapToLong(e -> lastOffset.getOrDefault(e.getKey(), 0L) - e.getValue())
                .sum());
          }

          consumer.close();
        } catch (Exception e) {
          throw new RuntimeException(e);
        } finally {
          consumer.close();
          closed.release();
        }
      });
    }

    public ConsumerRecord<K, V> lookUntil(Predicate<? super ConsumerRecord<K, V>> test) {
      return lookUntil(test, Duration.ofSeconds(5));
    }

    public ConsumerRecord<K, V> lookUntil(Predicate<? super ConsumerRecord<K, V>> test,
                                          Duration timeout) {
      Stopwatch timer = Stopwatch.createStarted();
      while (!timeout.minus(timer.elapsed()).isNegative()) {
        Optional<ConsumerRecord<K, V>> foundRecOpt =
            drainRecords().stream()
                .peek(r -> LOGGER.error(" {} ", r)).filter(test).findFirst();

        if (foundRecOpt.isPresent()) {
          return foundRecOpt.get();
        }
      }
      return null;
    }

    public List<ConsumerRecord<K, V>> drainRecords() {
      List<ConsumerRecord<K, V>> currRecords = new LinkedList<>();
      receivedRecords.drainTo(currRecords);
      return currRecords;
    }

    @Override
    public void close() {
      greenLight.set(false);
      try {
        closed.acquire();
      } catch (Exception e) {
        throw new RuntimeException(e);
      } finally {
        closed.release();
      }
    }
  }

  public static final class ProducerHelper<K, V> implements Closeable {
    private final Producer<K, V> producer;

    private ProducerHelper(Producer<K, V> producer) {
      this.producer = producer;
    }

    public RecordMetadata publish(String topic, K key, V value) {
      return publish(topic, key, value, r -> {
      });
    }


    public RecordMetadata publish(
        String topic, K key, V value,
        Consumer<? super ProducerRecord<K, V>> withRecord) {

      ProducerRecord<K, V> record = new ProducerRecord<>(topic, key, value);
      withRecord.accept(record);

      try {
        return producer.send(record, (meta, err) -> {
        }).get();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {
      producer.close();
    }
  }

  public static final class AdminHelper {
    private final AdminClient adminClient;
    private final AtomicInteger topicSequence;
    private final List<String> tempTopics;

    public AdminHelper(
        AdminClient adminClient,
        AtomicInteger topicSequence) {

      this.adminClient = adminClient;
      this.topicSequence = topicSequence;
      this.tempTopics = new LinkedList<>();
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
      String name = String.format("%s-%d", prefix, topicSequence.getAndIncrement());
      createTopic(name, partitions);
      return name;
    }

    public String safeTempTopic(String prefix, int partitions) {
      String name = String.format("%s-%d", prefix, topicSequence.getAndIncrement());
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

    public <R> R withAdminClientResults(RiskyFunction<R, AdminClient> fn) {
      try {
        return fn.process(adminClient);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }

    public void withAdminClient(RiskyAction<AdminClient> fn) {
      try {
        fn.accept(adminClient);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

    }

    public void deleteAllTopics() {
      deleteTopics(listTopics());
    }

    void deleteTempTopics() {
      if (!tempTopics.isEmpty()) {
        this.deleteTopics(tempTopics);
        tempTopics.clear();
      }
    }
  }

  @FunctionalInterface
  public interface RiskyFunction<R, T> {
    R process(T t) throws Exception;
  }

  @FunctionalInterface
  public interface RiskyAction<T> {
    void accept(T t) throws Exception;
  }
}
