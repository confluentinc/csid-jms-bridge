/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.ComponentLifeCycle;
import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.exchange.KafkaExchange.ExchangeChangeListener;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.ConsumerThread;
import io.confluent.amq.persistence.kafka.ConsumerThread.MessageReciever;
import io.confluent.amq.persistence.kafka.KafkaIO;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Aids in the actual passing of data from Kafka into the AMQ environment.
 */
public class KafkaExchangeEgress implements ExchangeChangeListener,
    MessageReciever<byte[], byte[]> {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeEgress.class));

  private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

  private final JmsBridgeConfiguration config;
  private final ConfluentAmqServer server;
  private final Map<String, KafkaTopicExchange> topicExchangeMap;
  private final KafkaIO kafkaIO;
  private final String hopsHeaderKey;

  private final ComponentLifeCycle state = new ComponentLifeCycle(SLOG);
  private volatile ConsumerThread<byte[], byte[]> consumerThread;

  public KafkaExchangeEgress(
      JmsBridgeConfiguration config,
      ConfluentAmqServer server,
      KafkaExchange kafkaExchange,
      KafkaIO kafkaIO) {

    this.config = config;
    this.server = server;
    this.kafkaIO = kafkaIO;
    this.topicExchangeMap = new ConcurrentHashMap<>();
    this.hopsHeaderKey = Headers.createHopsKey(config.getBridgeConfig().id());
    kafkaExchange.registerListener(this);

    state.doPrepare(() -> {
      //do nothing
    });
  }

  @Override
  public void onRecieve(ConsumerRecord<byte[], byte[]> kafkaRecord) {
    SLOG.trace(b -> b.event("ReceivedRecord")
        .addRecordMetadata(kafkaRecord));
    KafkaTopicExchange exchange = topicExchangeMap.get(kafkaRecord.topic());
    if (exchange != null) {

      Optional<Integer> maybeHopsVal = Headers.getIntHeader(hopsHeaderKey, kafkaRecord.headers());
      if (!maybeHopsVal.isPresent() || maybeHopsVal.get() < 1) {
        routeMessage(exchange, kafkaRecord);
      } else {
        SLOG.trace(b -> b.event("RecordNotRouted")
            .addRecordMetadata(kafkaRecord)
            .eventResult("MaxHopsMet")
            .putTokens("hops", maybeHopsVal.orElse(1)));
      }
    } else {
      SLOG.trace(b -> b.event("RecordNotRouted")
          .addRecordMetadata(kafkaRecord)
          .eventResult("NoExchangeFound"));
    }
  }

  private void routeMessage(
      KafkaTopicExchange exchange, ConsumerRecord<byte[], byte[]> kafkaRecord) {
    SLOG.trace(b -> b.event("RoutingRecord")
        .putTokens("topicAddress", exchange.amqAddressName())
        .putTokens("topic", kafkaRecord.topic()));

    CoreMessage coreMessage = new CoreMessage(
        server.getStorageManager().generateID(), kafkaRecord.serializedKeySize() + 50);

    KExMessageType msgType = KExMessageType
        .valueOf(exchange.originConfig().messageType().toUpperCase());

    coreMessage.setType((byte) msgType.getId());
    coreMessage.setDurable(true);
    coreMessage.setTimestamp(kafkaRecord.timestamp());
    coreMessage.setAddress(exchange.amqAddressName());
    coreMessage.putStringProperty(Headers.HDR_KAFKA_TOPIC, kafkaRecord.topic());
    coreMessage.putIntProperty(Headers.HDR_KAFKA_PARTITION, kafkaRecord.partition());
    coreMessage.putLongProperty(Headers.HDR_KAFKA_OFFSET, kafkaRecord.offset());
    coreMessage.putBytesProperty(Headers.HDR_KAFKA_KEY, kafkaRecord.key());

    Map<String, Object> jmsHeaders = Headers.convertHeaders(
        kafkaRecord.headers(),
        config.getBridgeConfig().id(),
        true);
    jmsHeaders.forEach(coreMessage::putObjectProperty);

    if (msgType == KExMessageType.BYTES) {
      coreMessage.getBodyBuffer().writeBytes(kafkaRecord.value());
    } else if (msgType == KExMessageType.TEXT) {
      String value = STRING_DESERIALIZER.deserialize("", kafkaRecord.value());
      coreMessage.getBodyBuffer().writeNullableSimpleString(new SimpleString(value));
    }

    try {
      server.getPostOffice().route(coreMessage, false);
    } catch (Exception e) {
      SLOG.trace(b -> b
          .event("RoutingRecord")
          .markFailure()
          .putTokens("topicAddress", exchange.amqAddressName())
          .putTokens("topic", kafkaRecord.topic()), e);

      throw new RuntimeException(e);
    }
  }

  public void start() {
    state.doStart(this::doStart);
  }

  public void doStart() {
    consumerThread = kafkaIO.startConsumerThread(b -> b
        .addAllTopics(topicExchangeMap.keySet())
        .groupId(config.getBridgeConfig().id() + ".exchange")
        .receiver(this)
        .pollMs(100L)
        .valueDeser(new ByteArrayDeserializer())
        .keyDeser(new ByteArrayDeserializer()));

  }

  public void stop() {
    state.doStop(this::doStop);
  }

  public void doStop() {
    if (consumerThread != null) {
      consumerThread.stop(true);
    }
  }

  public void updateConsumerTopics() {
    if (state.isStarted()) {
      SLOG.info(b -> b
          .event("UpdateConsumerTopics")
          .putTokens("topics", topicExchangeMap.keySet()));
      consumerThread.updateTopics(topicExchangeMap.keySet());
    }
  }

  @Override
  public void onAddExchange(KafkaTopicExchange topicExchange) {
    SLOG.debug(b -> b
        .event("AddExchange")
        .putTokens("exchange", topicExchange));

    topicExchangeMap.put(topicExchange.kafkaTopicName(), topicExchange);
    updateConsumerTopics();
  }

  @Override
  public void onRemoveExchange(KafkaTopicExchange topicExchange) {
    SLOG.debug(b -> b
        .event("RemoveExchange")
        .putTokens("exchange", topicExchange));

    topicExchangeMap.remove(topicExchange.kafkaTopicName(), topicExchange);
    updateConsumerTopics();
  }

  @Override
  public void onDisableExchange(KafkaTopicExchange topicExchange) {
    onRemoveExchange(topicExchange);
  }

  @Override
  public void onEnableExchange(KafkaTopicExchange topicExchange) {
    onAddExchange(topicExchange);
  }
}
