/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.exchange.Headers;
import io.confluent.amq.exchange.KExMessageType;
import io.confluent.amq.logging.StructuredLogger;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalRecordType;
import io.confluent.amq.persistence.kafka.journal.ProtocolRecordType;
import io.confluent.amq.persistence.kafka.journal.serde.JournalEntryKey;
import io.netty.buffer.Unpooled;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.core.buffers.impl.ChannelBufferWrapper;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Transformer;
import org.apache.kafka.streams.processor.ProcessorContext;

import java.util.*;

import static org.apache.activemq.artemis.utils.AbstractByteBufPool.DEFAULT_POOL_CAPACITY;

public class KafkaDivertProcessor implements
    Transformer<JournalEntryKey, JournalEntry, Iterable<KeyValue<byte[], byte[]>>> {

  private static final CoreMessageObjectPools POOLS =
      new CoreMessageObjectPools(128, DEFAULT_POOL_CAPACITY, 128, 128);

  private static final EnumSet<JournalRecordType> RECORDS_OF_INTEREST = EnumSet.of(
      JournalRecordType.ADD_RECORD
  );

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaDivertProcessor.class));

  private final Serializer<String> stringSerializer = Serdes.String().serializer();

  private final String bridgeId;
  private final String journalName;
  private ProcessorContext context;

  public KafkaDivertProcessor(
      String bridgeId,
      String journalName) {

    this.bridgeId = bridgeId;
    this.journalName = journalName;
  }

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  public boolean isDivertableMessage(JournalEntry entry) {
    return entry.hasAppendedRecord()
            && RECORDS_OF_INTEREST.contains(entry.getAppendedRecord().getRecordType())
            && ProtocolRecordType.ADD_MESSAGE_PROTOCOL.getValue() == entry.getAppendedRecord()
            .getProtocolRecordType();

  }

  @Override
  public Iterable<KeyValue<byte[], byte[]>> transform(JournalEntryKey key,
      JournalEntry entry) {

    if (entry == null
        || !isDivertableMessage(entry)) {

      SLOG.trace(b -> b
          .addJournalEntryKey(key)
          .addJournalEntry(entry)
          .event("RecordDrop"));
      return Collections.emptyList();
    }

    return process(key, entry);
  }

  @Override
  public void close() {
  }

  protected List<KeyValue<byte[], byte[]>> process(JournalEntryKey key, JournalEntry entry) {
    List<KeyValue<byte[], byte[]>> results = new LinkedList<>();
    Optional<ICoreMessage> amqMsgOpt = extractAmqMessage(entry);

    if (amqMsgOpt.isPresent()) {
      Optional<String> topic = kafkaTopicDestination(amqMsgOpt.get());

      if (topic.isPresent()) {

        SLOG.trace(b -> b
            .name(journalName)
            .event("DivertRecordToKafka")
            .addJournalEntryKey(key)
            .addJournalEntry(entry)
            .putTokens("kafkaTopic", topic.get()));

        //add routing header
        context.headers().add(
            Headers.EX_HDR_KAFKA_TOPIC,
            stringSerializer.serialize(null, topic.get()));

        //add timestamp header
        context.headers().add(
            Headers.EX_HDR_TS,
            stringSerializer.serialize(null, topic.get()));

        Optional<byte[]> kafkaKey = extractKey(amqMsgOpt.get());
        results.add(KeyValue.pair(
            kafkaKey.orElse(null),
            extractValue(amqMsgOpt.get()).orElse(null)));

        applyConvertedHeaders(amqMsgOpt.get(), context.headers());

      } else {
        SLOG.trace(b -> b
            .name(journalName)
            .event("DivertRecordToKafka")
            .markFailure()
            .addJournalEntryKey(key)
            .addJournalEntry(entry)
            .message("No kafka topic to forward to"));
      }

    } else {
      SLOG.trace(b -> b
          .name(journalName)
          .event("DivertRecordToKafka")
          .markFailure()
          .addJournalEntryKey(key)
          .addJournalEntry(entry)
          .message("Cannot extract AMQ message"));
    }

    return results;
  }

  public void applyConvertedHeaders(
      ICoreMessage coreMessage, org.apache.kafka.common.header.Headers kheaders) {

    Map<String, byte[]> headers = Headers.convertHeaders(coreMessage, bridgeId, true);
    headers.forEach(kheaders::add);
  }

  public Optional<ICoreMessage> extractAmqMessage(JournalEntry journalEntry) {

    ChannelBufferWrapper buff =
        new ChannelBufferWrapper(
            Unpooled.wrappedBuffer(journalEntry.getAppendedRecord().getData().toByteArray()),
            true);

    try {
      Message message = MessagePersister.getInstance()
          .decode(buff, null, POOLS, null);
      return Optional.of(message.toCore());
    } catch (Exception e) {
      SLOG.error(b -> b
          .name(journalName)
          .event("ExtractAmqMessage")
          .markFailure()
          .addJournalEntry(journalEntry), e);
      return Optional.empty();
    }
  }

  public Optional<String> kafkaTopicDestination(Message message) {
    return Optional.ofNullable(
        message.getStringProperty(Headers.EX_HDR_KAFKA_TOPIC));
  }

  public Optional<byte[]> extractKey(Message message) {

    if (message.containsProperty(Headers.EX_HDR_KAFKA_RECORD_KEY)) {
      byte[] value = message.getBytesProperty(Headers.EX_HDR_KAFKA_RECORD_KEY);
      message.removeProperty(Headers.EX_HDR_KAFKA_RECORD_KEY);
      return Optional.ofNullable(value);
    }
    return Optional.empty();
  }

  public Optional<byte[]> extractValue(ICoreMessage message) {
    switch (KExMessageType.fromId(message.getType())) {
      case BYTES:
        byte[] value = new byte[message.getBodyBufferSize()];
        BytesMessageUtil.bytesReadBytes(message.getReadOnlyBodyBuffer(), value);
        return Optional.of(value);
      case TEXT:
        return Optional.of(stringSerializer.serialize(
            null,
            TextMessageUtil.readBodyText(message.toCore().getBodyBuffer()).toString()));
      default:
        return Optional.empty();
    }
  }
}
