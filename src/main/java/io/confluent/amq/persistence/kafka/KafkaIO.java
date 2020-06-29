/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

public class KafkaIO {

  public static final boolean isKafkaMessage(ICoreMessage message) {
    return message.getPropertyNames().contains(KafkaRef.HEADER);
  }

  private static final Logger logger = Logger.getLogger(KafkaJournalStorageManager.class);
  private final static Set<Byte> SUPPORT_MSG_TYPES = new HashSet<>(
      Arrays.asList(Message.TEXT_TYPE, Message.BYTES_TYPE));

  private static final MessageAdapter NOP_MSG_ADAPTER = new MessageAdapter() {
    @Override
    public void receive(Message message) { /*nuthing*/ }

    @Override
    public Message transform(ConsumerRecord<byte[], byte[]> kafkaRecord) {
      return null;
    }
  };

  private volatile boolean run = false;
  private KafkaProducer<byte[], byte[]> kafkaProducer;
  private KafkaConsumer<byte[], byte[]> kafkaConsumer;
  private AdminClient adminClient;
  private StringSerializer stringSerializer = new StringSerializer();
  private LongSerializer longSerializer = new LongSerializer();

  private final SynchronousQueue<Collection<String>> topicReadQueue = new SynchronousQueue<>();
  private final Properties kafkaProps;
  private final Thread consumerThread;
  private Set<String> knownTopics;
  private AtomicReference<MessageAdapter> messageReciver = new AtomicReference<>(NOP_MSG_ADAPTER);

  public KafkaIO(Properties kafkaProps) {
    this.kafkaProps = kafkaProps;
    this.knownTopics = new HashSet<>();

    consumerThread = new Thread(this::consumerThread);
    consumerThread.setDaemon(true);
    consumerThread.setName("KafkaIO-Consumer-Thread");
  }

  public synchronized void start() {
    kafkaConsumer = new KafkaConsumer<>(kafkaProps, new ByteArrayDeserializer(),
        new ByteArrayDeserializer());
    kafkaProducer = new KafkaProducer<>(kafkaProps, new ByteArraySerializer(),
        new ByteArraySerializer());
    adminClient = AdminClient.create(kafkaProps);
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      kafkaProducer.close();
      adminClient.close();
    }));

    run = true;
    consumerThread.start();

    refreshTopics();
  }

  private void consumerThread() {
    Collection<String> topics = Collections.emptyList();
    while (run) {
      Collection<String> newTopics = null;
      if (topics.isEmpty()) {
        try {
          newTopics = topicReadQueue.poll(100, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
          //go around again
        }
      } else {
        newTopics = topicReadQueue.poll();
      }

      if (newTopics != null) {
        logger.info("###### Subscribing to topics: " + String.join(", ", newTopics));
        kafkaConsumer.subscribe(newTopics);
        topics = newTopics;
      }

      if (!topics.isEmpty()) {
        kafkaConsumer.poll(Duration.ofMillis(100)).forEach(r -> {
          filterAndTransformRecord(r).ifPresent(m -> messageReciver.get().receive(m));
        });
      }
    }
    kafkaConsumer.close();
  }

  protected Optional<Message> filterAndTransformRecord(ConsumerRecord<byte[], byte[]> record) {
    KafkaRef kref = new KafkaRef(record.topic(), record.partition(), record.offset());
    for (Header hdr : record.headers()) {
      if (KafkaRef.HEADER.equals(hdr.key())) {
        if (logger.isDebugEnabled()) {
          logger.info("Skipping message with " + KafkaRef.HEADER + " header: " + kref.asString());
        }
        return Optional.empty();
      }
    }

    Message message = messageReciver.get().transform(record);
    if (message == null) {
      return Optional.empty();
    }

    message.setAddress(record.topic());
    message.toCore().setType(Message.BYTES_TYPE);
    message.setCorrelationID(record.key());
    message.putStringProperty("KAFKA_REF", kref.asString());
    return Optional.of(message);
  }

  public void refreshTopics() {
    try {
      knownTopics.addAll(adminClient.listTopics().names().get());
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public Collection<String> getKnownTopics() {
    return knownTopics;
  }

  public void createTopic(String name, int partitions, int replication,
      Map<String, String> options) {
    NewTopic topic = new NewTopic(name, partitions, (short) replication);
    topic.configs(options);
    try {
      adminClient.createTopics(Collections.singletonList(topic)).all().get();
    } catch (ExecutionException | InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public void readTopics(Collection<String> topics, MessageAdapter receiver) {
    messageReciver.set(receiver);
    topicReadQueue.offer(topics);
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
      if (logger.isDebugEnabled()) {
        logger.debug("Writing message to kafka: " + message);
      }
      String correlationId = Objects.toString(message.getCorrelationID());
      String topic = message.getAddress();

      ProducerRecord<byte[], byte[]> krecord = null;
      if (correlationId != null) {
        krecord = new ProducerRecord<>(topic, correlationId.getBytes(), value);
      } else {
        krecord = new ProducerRecord<>(topic, value);
      }

      byte[] msgId = longSerializer.serialize("", message.getMessageID());
      krecord.headers().add("jms.MessageID", msgId);

      for (SimpleString hdrname : coreMessage.getPropertyNames()) {
        if (!hdrname.toString().startsWith("_")) {
          Object property = coreMessage.getBrokerProperty(hdrname);
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
            logger.warn("Setting header: " + propname);
            RecordHeader kheader = new RecordHeader(propname, propdata);
            krecord.headers().add(kheader);
          }
        }
      }

      kafkaProducer.send(krecord, (meta, err) -> {
        if (err != null) {
          future.completeExceptionally(err);
        } else {
          if (logger.isDebugEnabled()) {
            logger.info("Published kafka record metadata is: " + meta);
          }
          future.complete(new KafkaRef(meta.topic(), meta.partition(), meta.offset()));
        }
      });
    } else {
      future.complete(null);
    }

    return future;
  }

  public void stop() throws Exception {
    run = false;
    consumerThread.join();
    if (kafkaConsumer != null) {
      kafkaConsumer.close();
    }

    if (kafkaProducer != null) {
      kafkaProducer.close();
    }

    if (adminClient != null) {
      adminClient.close();
    }
  }

  public interface MessageAdapter {

    void receive(Message message);

    Message transform(ConsumerRecord<byte[], byte[]> kafkaRecord);
  }

  public static class KafkaRef {

    public static final String HEADER = "KAFKA_REF";

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
        throw new IllegalArgumentException("Invalid KafkaRef String: '" + asStringOutput + "'", e);
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
