/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.ComponentLifeCycle;
import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.ConsumerThread;
import io.confluent.amq.persistence.kafka.ConsumerThread.MessageReciever;
import io.confluent.amq.persistence.kafka.KafkaIO;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.jms.client.ActiveMQDestination;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Aids in the actual passing of data from Kafka into the AMQ environment.
 */
public class KafkaExchangeEgress implements MessageReciever<byte[], byte[]> {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeEgress.class));

  private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();

  private final BridgeConfig config;
  private final ConfluentAmqServer server;
  private final KafkaExchange kafkaExchange;
  private final KafkaIO kafkaIO;
  private final String hopsHeaderKey;

  private final ComponentLifeCycle state = new ComponentLifeCycle(SLOG);
  private volatile ConsumerThread<byte[], byte[]> consumerThread;

  public KafkaExchangeEgress(
      BridgeConfig config,
      ConfluentAmqServer server,
      KafkaExchange kafkaExchange,
      KafkaIO kafkaIO) {

    this.config = config;
    this.server = server;
    this.kafkaIO = kafkaIO;
    this.hopsHeaderKey = Headers.createHopsKey(config.id());
    this.kafkaExchange = kafkaExchange;

    state.doPrepare(() -> {
      //do nothing
    });
  }

  @Override
  public void onRecieve(ConsumerRecord<byte[], byte[]> kafkaRecord) {
    SLOG.trace(b -> b.event("ReceivedRecord")
        .addRecordMetadata(kafkaRecord));
    Optional<KafkaTopicExchange> exchangeOpt = kafkaExchange.findByTopic(kafkaRecord.topic());
    if (exchangeOpt.isPresent()) {

      //we allow one hop for incoming messages so that JMS consumers on the exchange address can
      //receive messages published by JMS producers to the exchange address.
      int hopsVal = Headers.getHopsValue(kafkaRecord.headers(), config.id());
      if (hopsVal < 1) {
        routeMessage(exchangeOpt.get(), kafkaRecord);
      } else {
        SLOG.trace(b -> b.event("RecordNotRouted")
            .addRecordMetadata(kafkaRecord)
            .eventResult("MaxHopsMet")
            .putTokens("hops", hopsVal));
      }
    } else {
      SLOG.trace(b -> b.event("RecordNotRouted")
          .addRecordMetadata(kafkaRecord)
          .eventResult("NoExchangeFound"));
    }
  }

  private void routeMessage(
      KafkaTopicExchange exchange, ConsumerRecord<byte[], byte[]> kafkaRecord) {

    String address = Headers.getStringHeader(
            Headers.createKafkaJmsPropKey(Headers.HDR_DESTINATION), kafkaRecord.headers())
        .map(ActiveMQDestination::fromPrefixedName)
        .map(ActiveMQDestination::getAddress)
        .orElse(exchange.amqAddressName());

    SLOG.trace(b -> b.event("RoutingRecord")
        .putTokens("topicAddress", address)
        .putTokens("topic", kafkaRecord.topic()));

    CoreMessage coreMessage = new CoreMessage(
        server.getStorageManager().generateID(), kafkaRecord.serializedKeySize() + 50);

    KExMessageType msgType = KExMessageType
        .valueOf(exchange.originConfig().messageType().toUpperCase());

    coreMessage.setType((byte) msgType.getId());
    coreMessage.setDurable(true);
    coreMessage.setTimestamp(kafkaRecord.timestamp());

    coreMessage.setAddress(address);
    coreMessage.putStringProperty(Headers.HDR_KAFKA_TOPIC, kafkaRecord.topic());
    coreMessage.putIntProperty(Headers.HDR_KAFKA_PARTITION, kafkaRecord.partition());
    coreMessage.putLongProperty(Headers.HDR_KAFKA_OFFSET, kafkaRecord.offset());
    coreMessage.putBytesProperty(Headers.HDR_KAFKA_KEY, kafkaRecord.key());

    Map<String, Object> jmsHeaders = Headers.convertHeaders(
        kafkaRecord.headers(),
        config.id(),
        true);
    jmsHeaders.forEach(coreMessage::putObjectProperty);

    if(kafkaRecord.value() != null) {
      if (msgType == KExMessageType.BYTES) {
        coreMessage.getBodyBuffer().writeBytes(kafkaRecord.value());
      } else if (msgType == KExMessageType.TEXT) {
        String value = STRING_DESERIALIZER.deserialize("", kafkaRecord.value());
        coreMessage.getBodyBuffer().writeNullableSimpleString(new SimpleString(value));
      }
    }

    try {
      RoutingStatus status = server.getPostOffice().route(coreMessage, false);
      SLOG.trace(b -> b
          .event("RoutingStatus")
          .addRecordMetadata(kafkaRecord)
          .putTokens("status", status));

    } catch (Exception e) {
      SLOG.trace(b -> b
          .event("RoutingRecord")
          .markFailure()
          .addRecordMetadata(kafkaRecord)
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
        .addAllTopics(kafkaExchange.allExchangeKafkaTopics())
        .groupId(config.id() + ".exchange")
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

  public Collection<String> currentConsumerTopics() {
    return consumerThread.currTopics();
  }

  public void updateConsumerTopics() {
    if (state.isStarted()) {
      Set<String> kafkaTopics = kafkaExchange.allReadyToReadKafkaTopics();
      SLOG.info(b -> b
          .event("UpdateConsumerTopics")
          .putTokens("topics", kafkaTopics));
      if (currentConsumerTopics().size() != kafkaTopics.size()
          || !currentConsumerTopics().containsAll(kafkaTopics)) {
        consumerThread.updateTopics(kafkaTopics);
      }
    }
  }
}
