/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.Route;
import io.confluent.amq.exchange.Headers;
import io.confluent.amq.filter.BridgeFilter;
import io.confluent.amq.filter.ExpressionFactory;
import io.confluent.amq.filter.FilterSupport;
import io.confluent.amq.filter.PropertyExtractor;
import io.confluent.amq.logging.LogSpec;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.journal.BaseJournalStreamTransformer;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;

public class AmqAddRecordProcessor extends BaseJournalStreamTransformer<byte[], byte[]> {

  public static final String TOPIC_ROUTING_KEY = "jms.kafka-routed-topic";
  private static final LongSerializer longSerializer = new LongSerializer();

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(AmqAddRecordProcessor.class));

  private final String bridgeId;
  private final RoutingConfig routingConfig;
  private final List<RouteHolder> validRoutes;

  public AmqAddRecordProcessor(
      String bridgeId, String journalName, String storeName, RoutingConfig routingConfig) {

    super(journalName, storeName);
    this.bridgeId = bridgeId;
    this.routingConfig = routingConfig;

    validRoutes = routingConfig.routes().stream()
        .map(this::initRoute)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(Collectors.toList());

    if (validRoutes.isEmpty()) {
      SLOG.warn(b -> b
          .event("AllRoutesInvalid")
          .message("No valid routes were configured, routing will be disabled.")
      );
    }
  }

  @Override
  public Iterable<KeyValue<byte[], byte[]>> transform(JournalEntryKey readOnlyKey,
      JournalEntry entry) {

    if (validRoutes.isEmpty()) {
      return Collections.emptyList();
    }

    //we only want messages
    if (!isAmqMessageEntry(entry)) {
      return Collections.emptyList();
    }

    ICoreMessage message = extractAmqMessage(entry);

    if (Headers.isKnown(message, bridgeId)) {

      SLOG.trace(b -> b.event("MessageKnown")
          .putAllTokens(message.toMap())
          .message("Message skipped since it has already been received by kafka"));

      return Collections.emptyList();
    }

    if (!isRouteableMessageType(message)) {
      return Collections.emptyList();
    }

    Optional<RouteHolder> routeMaybe = route(message);
    if (routeMaybe.isPresent()) {

      SLOG.trace(b -> b.event("MessageRouted")
          .markSuccess()
          .putTokens("route", routeMaybe.get().routeConfig)
          .putAllTokens(message.toMap())
          .message("Message successfully routed."));

      return convertMessage(routeMaybe.get(), message);
    } else {

      SLOG.trace(b -> b.event("MessageRouted")
          .markFailure()
          .putAllTokens(message.toMap())
          .message("No routes found matching message."));

      return Collections.emptyList();
    }
  }

  public List<KeyValue<byte[], byte[]>> convertMessage(RouteHolder route,
      ICoreMessage coreMessage) {

    getContext().headers().add(
        TOPIC_ROUTING_KEY,
        route.routeConfig.to().topic().getBytes(StandardCharsets.UTF_8));

    Map<String, byte[]> headers = Headers.convertHeaders(coreMessage, bridgeId, true);

    headers.forEach(getContext().headers()::add);

    Optional<byte[]> key = extractKey(route, coreMessage);
    if (!key.isPresent()) {
      handleMappingError(route, coreMessage, new LogSpec.Builder()
          .event("KeyExtraction")
          .message("No value for key selector found.")
          .putTokens("keySelector", route.routeConfig.map().key().orElse("MessageID"))
          .putAllTokens(coreMessage.toMap())
          .markFailure()
          .build());
      return Collections.emptyList();
    }

    return Collections.singletonList(
        KeyValue.pair(key.get(), extractValue(route, coreMessage)));
  }


  public Optional<byte[]> extractKey(RouteHolder route, ICoreMessage message) {
    if (route.keyExtractor != null) {
      Optional<Object> keyObj = route.keyExtractor.extract(wrapFilterSupport(message));
      return keyObj.flatMap(Headers::objectToBytes);
    } else {
      return Headers.objectToBytes(message.getMessageID());
    }
  }

  public byte[] extractValue(RouteHolder route, ICoreMessage message) {
    byte[] value = null;
    if (message.getType() == Message.TEXT_TYPE) {
      value = TextMessageUtil.readBodyText(message.getBodyBuffer()).toString()
          .getBytes(StandardCharsets.UTF_8);
    } else if (message.getType() == Message.BYTES_TYPE) {
      value = new byte[message.getBodyBufferSize()];
      BytesMessageUtil.bytesReadBytes(message.getReadOnlyBodyBuffer(), value);
    }
    return value;
  }

  public Optional<RouteHolder> route(ICoreMessage message) {
    final String address = message.getAddress();
    return validRoutes.stream()
        .filter(r -> Objects.equals(r.routeConfig.from().address(), address))
        .filter(r -> r.filter.match(wrapFilterSupport(message)))
        .findFirst();
  }

  public boolean isAmqMessageEntry(JournalEntry entry) {
    boolean isValid =
        entry != null
            && entry.hasAppendedRecord()
            && entry.getAppendedRecord().getRecordType() == JournalRecordType.ADD_RECORD
            && entry.getAppendedRecord().getProtocolRecordType()
            == ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue();

    if (!isValid) {
      SLOG.trace(b -> b.event("SkipIneligibleRecord")
          .addJournalEntry(entry));
    }

    return isValid;
  }

  public boolean isRouteableMessageType(ICoreMessage message) {
    boolean isValid = message.getType() == Message.TEXT_TYPE
        || message.getType() == Message.BYTES_TYPE;

    if (!isValid) {
      SLOG.debug(b -> b
          .event("SkipIneligibleMessage")
          .message("Only test and bytes messages are supported for routing.")
          .putTokens("messageType", message.getType())
      );
    }
    return isValid;
  }

  public ICoreMessage extractAmqMessage(JournalEntry entry) {
    //grab the payload and convert to a message
    ActiveMQBuffer amqBuffer = ActiveMQBuffers.wrappedBuffer(
        entry.getAppendedRecord().getData().toByteArray());

    Message message = MessagePersister.getInstance().decode(amqBuffer, null, null);

    return message.toCore();
  }

  public RoutingConfig getRoutingConfig() {
    return routingConfig;
  }

  public Optional<RouteHolder> initRoute(Route route) {
    boolean validRoute = route.from().address() != null
        && !route.from().address().matches("^\\s*$");

    if (!validRoute) {
      SLOG.warn(b -> b.event("ValidateRoute")
          .markFailure()
          .putTokens("route", route)
          .message("Invalid route found."));
      return Optional.empty();
    } else {
      SLOG.info(b -> b.event("ValidateRoute")
          .markSuccess()
          .putTokens("route", route)
          .message("Valid route found."));

      BridgeFilter filter;
      if (route.from().filter().isPresent()) {
        filter =
            ExpressionFactory.getInstance().parseAmqFilter(route.from().filter().get());
      } else {
        filter = BridgeFilter.FILTER_NONE;
      }

      PropertyExtractor keyExtractor = null;
      if (route.map().key().isPresent()) {
        keyExtractor = ExpressionFactory.getInstance()
            .parsePropertyExtractor(route.map().key().get());
      }
      return Optional.of(new RouteHolder(route, filter, keyExtractor));
    }
  }

  protected void handleMappingError(RouteHolder route, ICoreMessage message, LogSpec logSpec) {
    SLOG.error(b -> b.mergeFrom(logSpec));
  }

  private FilterSupport wrapFilterSupport(ICoreMessage message) {
    final Map<String, Object> props = Headers.getMessageProperties(message);
    return props::get;
  }

  static final class RouteHolder {

    final Route routeConfig;
    final BridgeFilter filter;
    final PropertyExtractor keyExtractor;

    RouteHolder(Route routeConfig, BridgeFilter filter,
        PropertyExtractor keyExtractor) {
      this.routeConfig = routeConfig;
      this.filter = filter;
      this.keyExtractor = keyExtractor;
    }
  }
}
