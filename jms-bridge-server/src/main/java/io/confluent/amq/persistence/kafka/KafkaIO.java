/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import com.google.protobuf.Message;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.kafka.ConsumerThread.Builder;
import io.confluent.amq.persistence.kafka.journal.JournalEntryKeyPartitioner;
import io.confluent.amq.persistence.kafka.journal.serde.ProtoSerializer;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaIO {

  private static final int CREATED = 0;
  private static final int STARTED = 1;
  private static final int STOPPED = 2;

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIO.class);
  private static final LogFormat LOG_FORMAT = LogFormat.forSubject("KafkaIO");

  private final Properties kafkaProps;
  private final StringSerializer stringSerializer = new StringSerializer();
  private final LongSerializer longSerializer = new LongSerializer();


  private final ReentrantReadWriteLock rwlock = new ReentrantReadWriteLock();
  private volatile int state = 0;
  private volatile KafkaProducer<? extends Message, ? extends Message> kafkaProducer;
  private volatile AdminClient adminClient;

  public static boolean isKafkaMessage(ICoreMessage message) {
    return message.getPropertyNames().contains(KafkaRef.SS_HEADER);
  }

  public KafkaIO(Properties kafkaProps) {
    this.kafkaProps = new Properties();
    this.kafkaProps.putAll(kafkaProps);
    this.kafkaProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
    this.kafkaProps.put(ProducerConfig.ACKS_CONFIG, "all");
    this.kafkaProps.put(
        ProducerConfig.PARTITIONER_CLASS_CONFIG,
        JournalEntryKeyPartitioner.class.getCanonicalName());

  }

  public KafkaIO(Map<String, Object> kafkaProps) {
    this(BridgeConfigFactory.propsToMap(kafkaProps));
  }


  public Properties getKafkaProps() {
    return kafkaProps;
  }

  public synchronized void start() {
    try {
      rwlock.writeLock().lock();
      if (state == CREATED || state == STOPPED) {
        ProtoSerializer<com.google.protobuf.Message> protoSerializer = new ProtoSerializer<>();
        kafkaProducer = new KafkaProducer<>(kafkaProps, protoSerializer, protoSerializer);
        adminClient = AdminClient.create(kafkaProps);
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

  public Map<TopicPartition, Long> fetchLatestOffsets(String topic) {
    return fetchWithAdminClient(admin -> {
      try {
        //get the partitions for the topic
        List<TopicPartition> tpList = adminClient
            .describeTopics(Collections.singleton(topic))
            .all()
            .get()
            .get(topic)
            .partitions()
            .stream()
            .map(tpi -> new TopicPartition(topic, tpi.partition()))
            .collect(Collectors.toList());

        //get the highwater marks (latest offsets)
        return adminClient
            .listOffsets(
                tpList.stream().collect(Collectors.toMap(tp -> tp, tp -> OffsetSpec.latest())))
            .all()
            .get()
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(Entry::getKey, v -> v.getValue().offset()));

      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  public void resetConsumerGroupOffsetsLatest(String consumerGroup, String topic) {
    withAdminClient(admin -> {
      try {
        Map<TopicPartition, OffsetAndMetadata> groupOffsetMap = fetchLatestOffsets(topic)
            .entrySet()
            .stream()
            .collect(
                Collectors.toMap(Entry::getKey, v -> new OffsetAndMetadata(v.getValue())));

        admin.alterConsumerGroupOffsets(
            consumerGroup,
            groupOffsetMap)
            .all()
            .get();
        //done moving offsets around ... whew!
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  /**
   * Creates a new consumer thread and starts it. It does not manage any more of it's lifecycle past
   * that point and is up to the caller to ensure it is stopped properly.
   */
  public <K, V> ConsumerThread<K, V> startConsumerThread(Consumer<Builder<K, V>> spec) {
    ConsumerThread.Builder<K, V> builder = ConsumerThread.newBuilder();
    kafkaProps.forEach((k, v) -> builder.putConsumerProps(k.toString(), v));
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
    return fetchWithAdminClient(this::listTopics);
  }

  private Set<String> listTopics(AdminClient admin) {
    try {
      return admin.listTopics().names().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Create the topic unless it already exists, if it does exist then do nothing.
   *
   * @return true if the topic was created false if it already exists
   */
  public boolean createTopicIfNotExists(String name, int partitions, int replication,
      Map<String, String> options) {

    return createTopic(name, partitions, replication, true, options);
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
   * @return true if the topic was created false if it was not (due to existence check)
   */
  private boolean createTopic(
      String name,
      int partitions,
      int replication,
      boolean checkIfExists,
      Map<String, String> options) {

    return fetchWithAdminClient(admin -> {
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
        adminClient.createTopics(Collections.singletonList(topic)).all().get();
      } catch (ExecutionException | InterruptedException e) {
        throw new RuntimeException(e);
      }

      return true;
    });
  }

  @SuppressWarnings("unchecked")
  public <K extends Message, V extends Message> void withProducer(
      Consumer<Producer<K, V>> produceFn) {

    try {
      rwlock.readLock().lock();
      if (state == STARTED) {
        produceFn.accept((Producer<K, V>) kafkaProducer);
      } else {
        throw new IllegalStateException("KafkaIO must be started before being used.");
      }
    } finally {
      rwlock.readLock().unlock();
    }
  }

  public <K, V> KafkaProducer<K, V> createProducer(Serializer<K> keySer, Serializer<V> valSer) {
    KafkaProducer<K, V> producer = new KafkaProducer<>(kafkaProps, keySer, valSer);
    Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
    return producer;
  }

  public void withAdminClient(Consumer<AdminClient> adminClientFn) {
    this.fetchWithAdminClient(admin -> {
      adminClientFn.accept(admin);
      return null;
    });
  }

  public <T> T fetchWithAdminClient(Function<AdminClient, T> adminClientFn) {
    try {
      rwlock.readLock().lock();
      if (state == STARTED) {
        return adminClientFn.apply(adminClient);
      } else {
        throw new IllegalStateException("KafkaIO must be started before being used.");
      }
    } finally {
      rwlock.readLock().unlock();
    }
  }


  /* //save for future reference
  public CompletableFuture<KafkaRef> writeMessage(Message message) {
    ICoreMessage coreMessage = message.toCore();

    final CompletableFuture<KafkaRef> future = new CompletableFuture<>();
    KafkaRef kafkaRef = null;
    byte[] value = null;
    if (coreMessage.getType() == Message.TEXT_TYPE) {
      value = TextMessageUtil.readBodyText(coreMessage.getBodyBuffer()).toString()
          .getBytes(StandardCharsets.UTF_8);
    } else if (coreMessage.getType() == Message.BYTES_TYPE) {
      value = new byte[coreMessage.getBodyBufferSize()];
      BytesMessageUtil.bytesReadBytes(coreMessage.getReadOnlyBodyBuffer(), value);
    }

    if (value != null) {
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Writing message to kafka: " + message);
      }
      String correlationId = Objects.toString(message.getCorrelationID(), null);
      String topic = message.getAddress();

      ProducerRecord<byte[], byte[]> krecord = null;
      if (correlationId != null) {
        krecord = new ProducerRecord<>(
            topic, correlationId.getBytes(StandardCharsets.UTF_8), value);
      } else {
        krecord = new ProducerRecord<>(topic, value);
      }

      convertHeaders(coreMessage).forEach(krecord.headers()::add);

      kafkaProducer.send(krecord, (meta, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          if (LOGGER.isDebugEnabled()) {
            LOGGER.info("Published kafka record metadata is: " + meta);
          }
          future.complete(new KafkaRef(meta.topic(), meta.partition(), meta.offset()));
        }
      });
    } else {
      future.complete(null);
    }

    return future;
  }
  */

  private List<RecordHeader> convertHeaders(ICoreMessage message) {
    final List<RecordHeader> kheaders = new LinkedList<>();
    byte[] msgId = longSerializer.serialize("", message.getMessageID());
    kheaders.add(new RecordHeader("jms.MessageID", msgId));

    for (SimpleString hdrname : message.getPropertyNames()) {
      if (!hdrname.toString().startsWith("_")) {
        Object property = message.getBrokerProperty(hdrname);
        String propname = hdrname.toString();
        byte[] propdata = null;
        if (property instanceof byte[]) {
          propdata = (byte[]) property;
        } else if (property != null) {
          propdata = stringSerializer.serialize("", property.toString());
        }

        if (propdata != null) {
          if (!propname.contains("KAFKA")) {
            propname = "jms." + propname;
          }
          LOGGER.warn("Setting header: " + propname);
          kheaders.add(new RecordHeader(propname, propdata));
        }
      }
    }

    return kheaders;
  }

  public void stop() {
    try {
      rwlock.writeLock().lock();
      if (state == STARTED) {
        kafkaProducer.close();
        adminClient.close();
      }
      state = STOPPED;
    } finally {
      rwlock.writeLock().unlock();
    }
  }

  public static class KafkaRef {

    public static final String HEADER = "KAFKA_REF";
    public static final SimpleString SS_HEADER = SimpleString.toSimpleString(HEADER);

    private final String topic;
    private final int partition;
    private final long offset;

    public KafkaRef(String topic, int partition, long offset) {
      this.topic = topic;
      this.partition = partition;
      this.offset = offset;
    }

    public KafkaRef(String asStringOutput) {
      String[] parts = asStringOutput.split("\\|");
      if (parts.length != 3) {
        throw new IllegalArgumentException("Invalid KafkaRef String: '" + asStringOutput + "'");
      }
      try {
        this.topic = parts[0];
        this.partition = Integer.parseInt(parts[1]);
        this.offset = Long.parseLong(parts[2]);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Invalid KafkaRef String: '" + asStringOutput + "'",
            e);
      }
    }

    public String getTopic() {
      return topic;
    }

    public int getPartition() {
      return partition;
    }

    public long getOffset() {
      return offset;
    }

    public String asString() {
      return topic + '|' + partition + '|' + offset;
    }

    @Override
    public String toString() {
      return asString();
    }
  }
}
