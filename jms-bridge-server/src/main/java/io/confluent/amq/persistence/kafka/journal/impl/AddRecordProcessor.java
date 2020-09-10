/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.Convert;
import io.confluent.amq.config.RoutingConfig.Route;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.journal.BaseJournalStreamTransformer;
import io.confluent.amq.persistence.kafka.journal.JournalStreamTransformer;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQBuffers;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.streams.KeyValue;

public class AddRecordProcessor extends BaseJournalStreamTransformer<byte[], byte[]> {
  public static final String TOPIC_ROUTING_KEY = "jms.kafka-routed-topic";
  private static final LongSerializer longSerializer = new LongSerializer();

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(AddRecordProcessor.class));

  private final RoutingConfig routingConfig;
  private final List<Route> validRoutes;

  public AddRecordProcessor(String journalName, String storeName, RoutingConfig routingConfig) {
    super(journalName, storeName);
    this.routingConfig = routingConfig;

    validRoutes = routingConfig.routes().stream()
        .filter(this::validateRoute)
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

    if (!isRouteableMessageType(message)) {
      return Collections.emptyList();
    }

    Optional<Route> routeMaybe = route(message);
    if (routeMaybe.isPresent()) {
      SLOG.trace(b -> b.event("MessageRouted")
          .markSuccess()
          .putTokens("route", routeMaybe.get())
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

  public List<KeyValue<byte[], byte[]>> convertMessage(Route route, ICoreMessage coreMessage) {
    getContext().headers().add(
        TOPIC_ROUTING_KEY,
        route.to().topic().getBytes(StandardCharsets.UTF_8));

    byte[] value = null;
    if (coreMessage.getType() == Message.TEXT_TYPE) {
      value = TextMessageUtil.readBodyText(coreMessage.getBodyBuffer()).toString()
          .getBytes(StandardCharsets.UTF_8);
    } else if (coreMessage.getType() == Message.BYTES_TYPE) {
      value = new byte[coreMessage.getBodyBufferSize()];
      BytesMessageUtil.bytesReadBytes(coreMessage.getReadOnlyBodyBuffer(), value);
    }

      convertHeaders(coreMessage).forEach(krecord.headers()::add);
    }


  public byte[] extractKey(Convert conversion, ICoreMessage message) {
    byte[] correlationId = MessageUtil.getJMSCorrelationIDAsBytes(message);
    if (correlationId != null && correlationId.length > 0) {
      return correlationId;
    }



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

  public Optional<Route> route(ICoreMessage message) {
    final String address = message.getAddress();
    return validRoutes.stream()
        .filter(r -> Objects.equals(r.from().address(), address))
        .findFirst();
  }

  public boolean isAmqMessageEntry(JournalEntry entry) {
    boolean isValid =
        entry != null
            && entry.hasAppendedRecord()
            && entry.getAppendedRecord().getRecordType() == JournalRecordType.ADD_RECORD
            && entry.getAppendedRecord().getProtocolRecordType() ==
            ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue();

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
        entry.getAppendedRecord().getData().asReadOnlyByteBuffer());

    Message message = MessagePersister.getInstance().decode(amqBuffer, null, null);

    return message.toCore();
  }

  public RoutingConfig getRoutingConfig() {
    return routingConfig;
  }

  public boolean validateRoute(Route route) {
    boolean validRoute = route.from().address() != null
        && !route.from().address().matches("^\\s*$");

    if (!validRoute) {
      SLOG.warn(b -> b.event("ValidateRoute")
          .markFailure()
          .putTokens("route", route)
          .message("Invalid route found."));
    } else {
      SLOG.info(b -> b.event("ValidateRoute")
          .markSuccess()
          .putTokens("route", route)
          .message("Valid route found."));
    }

    return validRoute;
  }
}
