/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import io.confluent.amq.logging.LogFormat;
import io.confluent.amq.persistence.kafka.ConsumerThread.Builder;
import io.confluent.amq.persistence.kafka.journal.KafkaJournalRecord;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import org.apache.activemq.artemis.api.core.ActiveMQExceptionType;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaIO {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaIO.class);
  private static final LogFormat LOG_FORMAT = LogFormat.forSubject("KafkaIO");

  private static final ThreadGroup THREAD_GROUP;

  static {
    THREAD_GROUP = new ThreadGroup("kafkaIo-consumers");
    THREAD_GROUP.setDaemon(true);
  }

  private final Properties kafkaProps;

  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private AdminClient adminClient;
  private StringSerializer stringSerializer = new StringSerializer();
  private LongSerializer longSerializer = new LongSerializer();

  public static boolean isKafkaMessage(ICoreMessage message) {
    return message.getPropertyNames().contains(KafkaRef.SS_HEADER);
  }

  public KafkaIO(Properties kafkaProps) {
    this.kafkaProps = kafkaProps;
  }

  public Properties getKafkaProps() {
    return kafkaProps;
  }

  public synchronized void start() {
    kafkaProducer = new KafkaProducer<>(kafkaProps, new ByteArraySerializer(),
        new ByteArraySerializer());
    adminClient = AdminClient.create(kafkaProps);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaProducer.close();
      adminClient.close();
    }));
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

    Thread thread = new Thread(THREAD_GROUP, consumerThread,
        "kafka-consumer-" + consumerThread.groupId());

    thread.start();
    return consumerThread;
  }

  public Set<String> listTopics() {
    try {
      return adminClient.listTopics().names().get();
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

    if (listTopics().contains(name)) {
      return false;
    } else {
      createTopic(name, partitions, replication, options);
      return true;
    }
  }

  public void createTopic(String name, int partitions, int replication,
      Map<String, String> options) {

    LOGGER.info(LOG_FORMAT.build(b -> b
        .event("CreateTopic")
        .putTokens("topic", name)
        .putTokens("partitions", partitions)
        .putTokens("replication", replication)));

    NewTopic topic = new NewTopic(name, partitions, (short) replication);
    topic.configs(options);
    try {
      adminClient.createTopics(Collections.singletonList(topic)).all().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void writeJournalRecord(final KafkaJournalRecord record) {
    int pcount = kafkaProducer.partitionsFor(record.getDestTopic()).size();
    byte[] partitionKey = record.getKafkaPartitionKey().toByteArray();
    int partition = Utils.toPositive(Utils.murmur2(partitionKey)) % pcount;

    ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(
        record.getDestTopic(),
        partition,
        record.getKafkaMessageKey().toByteArray(),
        record.getRecord().toByteArray());

    if (LOGGER.isTraceEnabled()) {
      LOGGER.trace(LOG_FORMAT.build(b -> b
          .event("PublishJournalRecord")
          .addProducerRecord(producerRecord)
          .addJournalRecordKey(record.getKafkaMessageKey())
          .addJournalRecord(record.getRecord())));
    }

    kafkaProducer.send(producerRecord, (meta, err) -> {
      if (err != null) {
        if (record.getIoCompletion() != null) {
          record.getIoCompletion()
              .onError(ActiveMQExceptionType.IO_ERROR.getCode(), err.getMessage());
        }

        LOGGER.error(LOG_FORMAT.build(b -> b
            .event("PublishJournalRecord")
            .markFailure()
            .addProducerRecord(producerRecord)
            .addJournalRecordKey(record.getKafkaMessageKey())
            .addJournalRecord(record.getRecord())), err);

      } else {

        if (LOGGER.isTraceEnabled()) {
          LOGGER.trace(LOG_FORMAT.build(b -> b
              .event("PublishJournalRecord")
              .markSuccess()
              .addRecordMetadata(meta)
              .addJournalRecordKey(record.getKafkaMessageKey())
              .addJournalRecord(record.getRecord())));
        }

        if (record.getIoCompletion() != null) {
          record.getIoCompletion().done();
        }
      }
    });
  }

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

  public void stop() throws Exception {

    if (kafkaProducer != null) {
      kafkaProducer.close();
    }

    if (adminClient != null) {
      adminClient.close();
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
