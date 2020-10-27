/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.ComponentLifeCycle;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIO;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.impl.QueueImpl;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Aids in the actual movement of data from AMQ to Kafka.
 */
public class KafkaExchangeIngress implements Consumer, KafkaExchange.ExchangeChangeListener {

  private static final EnumSet<KExMessageType> ROUTEABLE_MESSAGE_TYPES = EnumSet.of(
      KExMessageType.BYTES,
      KExMessageType.TEXT
  );

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeIngress.class));


  private final BridgeConfig config;
  private final KafkaProducer<byte[], byte[]> producer;
  private final KafkaExchange exchange;
  private final java.util.Map<Long, MessageReference> refs = new LinkedHashMap<>();
  private final Map<String, KafkaTopicExchange> queueExchangemap;
  private final Long sequentialId;
  private final ComponentLifeCycle state = new ComponentLifeCycle(SLOG);

  public KafkaExchangeIngress(
      BridgeConfig config,
      KafkaExchange kafkaExchange,
      KafkaIO kafkaIO,
      Long sequentialId) {

    this.config = config;
    this.exchange = kafkaExchange;
    this.producer = kafkaIO.createProducer(new ByteArraySerializer(), new ByteArraySerializer());
    this.sequentialId = sequentialId;

    this.queueExchangemap = new ConcurrentHashMap<>();
    exchange.registerListener(this);

    state.doPrepare(() -> {
      //do nothing
    });
  }

  @Override
  public void onAddExchange(KafkaTopicExchange topicExchange) {
    SLOG.debug(b -> b.event("AddExchange").putTokens("exchange", topicExchange));
    queueExchangemap.put(topicExchange.ingressQueueName(), topicExchange);
  }

  @Override
  public void onRemoveExchange(KafkaTopicExchange topicExchange) {
    SLOG.debug(b -> b.event("RemoveExchange").putTokens("exchange", topicExchange));
    queueExchangemap.remove(topicExchange.ingressQueueName());
  }

  @Override
  public HandleStatus handle(MessageReference reference) {
    if (!state.isStarted()) {
      SLOG.debug(b -> b
          .event("HandleMessage")
          .eventResult("Busy")
          .message("Exchange has not been started."));
      return HandleStatus.BUSY;
    }

    String originalAddress = reference.getQueue().getAddress().toString();
    KafkaTopicExchange topicExchange = queueExchangemap.get(originalAddress);
    if (topicExchange == null) {
      SLOG.debug(b -> b
          .event("HandleMessage")
          .eventResult("NoMatch")
          .message("No KafkaTopicExchange exists matching address.")
          .putTokens("address", originalAddress));
      return HandleStatus.NO_MATCH;
    }

    ICoreMessage message = reference.getMessage().toCore();
    if (!isRouteableMessageType(message)) {
      SLOG.debug(b -> b
          .event("HandleMessage")
          .eventResult("NoMatch")
          .message("Message is not of a supported message type.")
          .putTokens("supportedMessageTypes", ROUTEABLE_MESSAGE_TYPES
              .stream()
              .map(KExMessageType::name)
              .collect(Collectors.joining(", ")))
          .putTokens("messageType", KExMessageType.fromId(message.getType())));

      return HandleStatus.NO_MATCH;
    }

    if (!this.exchange.exchangeWriteable(topicExchange)) {
      SLOG.debug(b -> b
          .event("HandleMessage")
          .eventResult("Busy")
          .putTokens("address", originalAddress)
          .message("Exchange for address is currently paused."));
      return HandleStatus.BUSY;
    }

    reference.handled();

    synchronized (refs) {
      refs.put(reference.getMessageID(), reference);
    }

    return route(topicExchange, reference);
  }

  @Override
  public void proceedDeliver(MessageReference reference) throws Exception {
    //do nothing
  }

  @Override
  public Filter getFilter() {
    return null;
  }

  @Override
  public List<MessageReference> getDeliveringMessages() {
    synchronized (refs) {
      return new ArrayList<>(refs.values());
    }
  }

  @Override
  public String debug() {
    return toString();
  }

  @Override
  public String toManagementString() {
    return this.getClass().getSimpleName() + "[]";
  }

  @Override
  public void disconnect() {
    //do nothing
  }

  @Override
  public long sequentialID() {
    return sequentialId;
  }

  public void start() {
    state.doStart(() -> {
      //nothing needed to do
    });
  }

  public void stop() {
    state.doStop(() -> {
      //nothing needed to do
    });
  }

  public HandleStatus route(
      KafkaTopicExchange route,
      MessageReference reference) {

    SLOG.trace(b -> b
        .event("RouteMessage")
        .putTokens("exchange", route)
        .putTokens("message", reference.getMessageID()));

    ICoreMessage coreMessage = reference.getMessage().toCore();
    String bridgeId = config.id();

    Map<String, byte[]> headers = Headers.convertHeaders(coreMessage, bridgeId, true);
    final byte[] key = extractKey(route, headers);
    final byte[] value = extractValue(route, coreMessage);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        route.kafkaTopicName(), key, value);

    headers.forEach(record.headers()::add);

    try {
      RecordMetadata meta = producer.send(record).get();
      SLOG.trace(b -> b
          .event("PublishRoutedMessage")
          .markSuccess()
          .addProducerRecord(record)
          .addRecordMetadata(meta));
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("PublishRoutedMessage")
          .markFailure(), e);

      if (e instanceof SerializationException) {
        return HandleStatus.HANDLED;
      }

      // The delivering count should also be decreased as to avoid inconsistencies
      ((QueueImpl) reference.getQueue()).decDelivering(reference);

      //we will retry this later.
      return HandleStatus.BUSY;
    } finally {

      synchronized (refs) {
        refs.remove(coreMessage.getMessageID());
      }
      coreMessage.usageDown();
    }

    return HandleStatus.HANDLED;
  }


  public byte[] extractKey(KafkaTopicExchange exchange, Map<String, byte[]> headers) {
    byte[] key = headers.get(Headers.createKafkaJmsPropKey(Headers.HDR_CORRELATION_ID));
    if (key == null) {
      key = headers.get(Headers.createKafkaJmsPropKey(exchange.originConfig().keyProperty()));
      if (key == null) {
        key = headers.get(Headers.createKafkaJmsPropKey(Headers.HDR_MESSAGE_ID));
      }
    }
    return key;
  }

  public byte[] extractValue(KafkaTopicExchange route, ICoreMessage message) {
    switch (KExMessageType.fromId(message.getType())) {
      case BYTES:
        byte[] value = new byte[message.getBodyBufferSize()];
        BytesMessageUtil.bytesReadBytes(message.getReadOnlyBodyBuffer(), value);
        return value;
      case TEXT:
        return Headers.toBytes(TextMessageUtil.readBodyText(message.getBodyBuffer()).toString());
      default:
        return null;
    }
  }

  public boolean isRouteableMessageType(ICoreMessage message) {
    return ROUTEABLE_MESSAGE_TYPES.contains(KExMessageType.fromId(message.getType()));
  }
}
