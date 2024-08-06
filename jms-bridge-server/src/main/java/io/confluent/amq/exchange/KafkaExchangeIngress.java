/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.kafka.KafkaIO;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class KafkaExchangeIngress implements Consumer {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeIngress.class));

  private static final EnumSet<KExMessageType> ROUTEABLE_MESSAGE_TYPES = EnumSet.of(
      KExMessageType.BYTES,
      KExMessageType.TEXT
  );

  private boolean active;

  private final Queue queue;

  private final long sequentialID;

  private final BridgeConfig config;

  private final KafkaTopicExchange exchange;

  private final KafkaIO kafkaIO;

  public KafkaExchangeIngress(
      BridgeConfig config,
      KafkaTopicExchange kafkaTopicExchange,
      KafkaIO kafkaIO,
      Queue queue,
      long sequentialID) {

    this.config = config;

    this.exchange = kafkaTopicExchange;

    this.kafkaIO = kafkaIO;

    this.queue = queue;

    this.sequentialID = sequentialID;

  }

  @Override
  public long sequentialID() {
    return sequentialID;
  }

  @Override
  public Filter getFilter() {
    return null;
  }

  @Override
  public String debug() {
    return toString();
  }

  @Override
  public String toManagementString() {
    return "KafkaExchangeIngress[" + queue.getName() + "/" + queue.getID() + "]";
  }

  @Override
  public void disconnect() {
    //noop
  }

  public synchronized void start() throws Exception {
    active = true;
    SLOG.trace(b -> b
        .event("Start")
        .putTokens("queue", queue));
    queue.addConsumer(this);
    queue.deliverAsync();
  }

  public synchronized void stop() throws Exception {
    active = false;
    SLOG.trace(b -> b
        .event("Stop")
        .putTokens("queue", queue));
    queue.removeConsumer(this);
  }

  public synchronized void close() {
    active = false;
    SLOG.trace(b -> b
        .event("Close")
        .putTokens("queue", queue));
    queue.removeConsumer(this);
  }

  @Override
  public synchronized HandleStatus handle(final MessageReference reference) throws Exception {
    KafkaProducer<byte[], byte[]> producer = kafkaIO.getExternalProducer();
    if (!active || null == producer) {
      SLOG.trace(b -> b
          .event("RouteMessage")
          .markFailure()
          .eventResult("NotReady")
          .putTokens("exchange", exchange)
          .putTokens("producerIsNull", null == producer)
          .putTokens("message", reference.getMessageID()));
      return HandleStatus.BUSY;
    } else if (!isRouteableMessageType(reference.getMessage())) {
      //add metric for unrouteable messages
      //add metric for routed messages
      SLOG.trace(b -> b
          .event("RouteMessage")
          .markFailure()
          .eventResult("WrongMessageType")
          .putTokens("exchange", exchange)
          .putTokens("message", reference.getMessageID()));
      return HandleStatus.NO_MATCH;
    }

    //add metric for routed messages
    SLOG.trace(b -> b
        .event("RouteMessage")
        .putTokens("exchange", exchange)
        .putTokens("message", reference.getMessageID()));

    ICoreMessage coreMessage = reference.getMessage().toCore();
    String bridgeId = config.id();

    Map<String, byte[]> headers = Headers.convertHeaders(coreMessage, bridgeId, true);
    byte[] key = extractKey(headers);
    byte[] value = extractValue(coreMessage);
    ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
        exchange.kafkaTopicName(), key, value);

    headers.forEach(record.headers()::add);

    producer.send(record, ack(reference));

    //get more messages
    queue.deliverAsync();

    return HandleStatus.HANDLED;
  }

  @Override
  public void proceedDeliver(MessageReference reference) {
    // no op
  }

  /* (non-Javadoc)
   * @see org.apache.activemq.artemis.core.server.Consumer#getDeliveringMessages()
   */
  @Override
  public List<MessageReference> getDeliveringMessages() {
    return Collections.emptyList();
  }


  private Callback ack(MessageReference reference) {
    return (RecordMetadata meta, Exception err) -> {
      SLOG.trace(b -> b
          .event("PublishRoutedMessage")
          .markSuccess()
          .putTokens("messageId", reference.getMessageID())
          .addRecordMetadata(meta));

      Exception ourErr = err;
      if (null == ourErr) {
        reference.handled();

        try {
          queue.acknowledge(reference);
        } catch (Exception e) {
          ourErr = e;
        }
        //add metric for successful publishes
      }

      if (null != ourErr) {
        SLOG.error(b -> b
            .event("PublishRoutedMessage")
            .markFailure(), ourErr);
        //add metric for failed publishes
      }
    };
  }

  public byte[] extractKey(Map<String, byte[]> headers) {
    byte[] key = headers.get(Headers.createKafkaJmsPropKey(Headers.HDR_CORRELATION_ID));
    if (null == key) {
      key = headers.get(Headers.createKafkaJmsPropKey(exchange.originConfig().keyProperty()));
      if (null == key) {
        key = headers.get(Headers.createKafkaJmsPropKey(Headers.HDR_MESSAGE_ID));
      }
    }
    return key;
  }

  @Nullable
  public byte[] extractValue(ICoreMessage message) {
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

  public boolean isRouteableMessageType(Message message) {
    return ROUTEABLE_MESSAGE_TYPES.contains(KExMessageType.fromId(message.toCore().getType()));
  }

}
