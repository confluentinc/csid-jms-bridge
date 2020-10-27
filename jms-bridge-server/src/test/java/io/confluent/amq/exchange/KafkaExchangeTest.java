/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.config.RoutingConfig.RoutedTopic.Builder;
import io.confluent.amq.exchange.KafkaExchange.ExchangeChangeListener;
import org.junit.jupiter.api.Test;

class KafkaExchangeTest {
  static final KafkaTopicExchange KTE_STUB = new KafkaTopicExchange.Builder()
      .kafkaTopicName("kafka-topic")
      .ingressQueueName("kafka.kafka-topic.forward")
      .amqAddressName("kafka.kafka-topic")
      .mutateOriginConfig(o -> o.match("kafka-topic"))
      .build();

  KafkaExchange subject = new KafkaExchange();

  @Test
  public void addedExchangeWithZeroReadersIsNotReadable() {
    subject.addTopicExchange(KTE_STUB, true, 0);
    assertFalse(subject.exchangeReadable(KTE_STUB));
    assertTrue(subject.exchangeWriteable(KTE_STUB));
  }

  @Test
  public void addedExchangeWithOneReaderIsReadable() throws Exception {
    subject.addTopicExchange(KTE_STUB, true, 1);
    assertTrue(subject.exchangeReadable(KTE_STUB));
    assertTrue(subject.exchangeWriteable(KTE_STUB));
  }

  @Test
  public void addedExchangeWithZeroReaderAlwaysConsume() throws Exception {
    KafkaTopicExchange kte = KafkaTopicExchange.Builder.from(KTE_STUB)
        .mutateOriginConfig(o -> o.consumeAlways(true))
        .build();

    subject.addTopicExchange(kte, true, 0);
    assertTrue(subject.exchangeReadable(KTE_STUB));
    assertTrue(subject.exchangeWriteable(KTE_STUB));
  }

  @Test
  public void exchangeReaderCountsVary() throws Exception {
    subject.addTopicExchange(KTE_STUB, true, 0);
    assertFalse(subject.exchangeReadable(KTE_STUB));

    subject.addReader(KTE_STUB.amqAddressName());
    assertTrue(subject.exchangeReadable(KTE_STUB));

    subject.removeReader(KTE_STUB.amqAddressName());
    assertFalse(subject.exchangeReadable(KTE_STUB));
  }

  @Test
  public void listenerInvokedAsExchangesChange() throws Exception {
    ExchangeChangeListener mockListener1 = mock(ExchangeChangeListener.class);
    subject.registerListener(mockListener1);
    subject.addTopicExchange(KTE_STUB, true, 0);

    verify(mockListener1).onAddExchange(KTE_STUB);

    subject.addReader(KTE_STUB.amqAddressName());
    verify(mockListener1).onEnableExchange(KTE_STUB);

    subject.removeReader(KTE_STUB.amqAddressName());
    verify(mockListener1).onDisableExchange(KTE_STUB);

    subject.removeExchange(KTE_STUB);
    verify(mockListener1).onRemoveExchange(KTE_STUB);
  }

  @Test
  public void testWriteDisableEnable() throws Exception {
    subject.addTopicExchange(KTE_STUB, true, 0);
    assertTrue(subject.exchangeWriteable(KTE_STUB));

    subject.disableWrite(KTE_STUB);
    assertFalse(subject.exchangeWriteable(KTE_STUB));

    subject.enableWrite(KTE_STUB);
    assertTrue(subject.exchangeWriteable(KTE_STUB));

    subject.addTopicExchange(KTE_STUB, false, 0);
    assertFalse(subject.exchangeWriteable(KTE_STUB));
  }

}