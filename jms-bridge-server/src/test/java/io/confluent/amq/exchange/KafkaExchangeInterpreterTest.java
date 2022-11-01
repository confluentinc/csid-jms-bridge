/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.test.TestSupport;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.hamcrest.CoreMatchers;

import java.nio.charset.StandardCharsets;
import java.util.function.Consumer;
import java.util.function.UnaryOperator;

import static org.junit.jupiter.api.Assertions.*;

class KafkaExchangeInterpreterTest {
    String amqAddressName = "amqAddressName";
    String kafkaTopicName = "kafkaTopicName";
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
            .id("test")
            .routing(new RoutingConfig.Builder().buildPartial())
            .buildPartial();

    public KafkaTopicExchange defaultExchange() {
        return defaultExchange(UnaryOperator.identity());
    }

    public KafkaTopicExchange defaultExchange(UnaryOperator<RoutingConfig.RoutedTopic.Builder> rtCfgModifier) {
        return new KafkaTopicExchange.Builder()
                .amqAddressName(amqAddressName)
                .kafkaTopicName(kafkaTopicName)
                .originConfig(
                        rtCfgModifier.apply(
                                new RoutingConfig.RoutedTopic.Builder()
                                    .match("kafkaTopicName")))
                .build();
    }

    @Test
    public void testExtractKey_Default() throws Exception {
        KafkaTopicExchange kte = defaultExchange();
        KafkaExchangeInterpreter kafkaExchangeInterpreter = new KafkaExchangeInterpreter(bridgeConfig);
            CoreMessage message = new CoreMessage(1L, "body".length() + 50);
            message.setAddress("address");
            //message.setRoutingType(RoutingType.MULTICAST);
            message.setDurable(true);
            message.setType(Message.BYTES_TYPE);
            //message.setTimestamp(TS_FIXTURE);
           // message.setCorrelationID("correlation-id");
            message.setMessageID(1L);

            message.getBodyBuffer().writeString("body");

        byte[] keyBytes = kafkaExchangeInterpreter.extractKey(kte, message);
        Long key = TestSupport.longDeserializer().deserialize(null, keyBytes);
        assertEquals(1L, key);
    }

    @Test
    public void testExtractKey_CorrelationIdDefault() throws Exception {
        KafkaTopicExchange kte = defaultExchange();
        KafkaExchangeInterpreter kafkaExchangeInterpreter = new KafkaExchangeInterpreter(bridgeConfig);
        CoreMessage message = new CoreMessage(1L, "body".length() + 50);
        message.setAddress("address");
        //message.setRoutingType(RoutingType.MULTICAST);
        message.setDurable(true);
        message.setType(Message.BYTES_TYPE);
        //message.setTimestamp(TS_FIXTURE);
        message.setCorrelationID("correlation-id");
        message.setMessageID(1L);

        message.getBodyBuffer().writeString("body");

        byte[] keyBytes = kafkaExchangeInterpreter.extractKey(kte, message);
        String key = TestSupport.stringDeserializer().deserialize(null, keyBytes);
        assertEquals("correlation-id", key);
    }

    @Test
    public void testExtractKey_CorrelationIdOverrideOff() throws Exception {
        KafkaTopicExchange kte = defaultExchange(cfg ->
                cfg.correlationKeyOverride(false));

        KafkaExchangeInterpreter kafkaExchangeInterpreter = new KafkaExchangeInterpreter(bridgeConfig);
        CoreMessage message = new CoreMessage(1L, "body".length() + 50);
        message.setAddress("address");
        message.setDurable(true);
        message.setType(Message.BYTES_TYPE);
        message.setCorrelationID("correlation-id");
        message.setMessageID(1L);

        message.getBodyBuffer().writeString("body");

        byte[] keyBytes = kafkaExchangeInterpreter.extractKey(kte, message);
        Long key = TestSupport.longDeserializer().deserialize(null, keyBytes);
        assertEquals(1L, key);
    }

    @Test
    public void testExtractKey_keyPropertyWithCorrelationId() throws Exception {
        KafkaTopicExchange kte = defaultExchange(cfg ->
                cfg.keyProperty("my-key"));

        KafkaExchangeInterpreter kafkaExchangeInterpreter = new KafkaExchangeInterpreter(bridgeConfig);
        CoreMessage message = new CoreMessage(1L, "body".length() + 50);
        message.putStringProperty("my-key", "super-keytastic!");
        message.setAddress("address");
        message.setDurable(true);
        message.setType(Message.BYTES_TYPE);
        message.setCorrelationID("correlation-id");
        message.setMessageID(1L);

        message.getBodyBuffer().writeString("body");

        byte[] keyBytes = kafkaExchangeInterpreter.extractKey(kte, message);
        String key = TestSupport.stringDeserializer().deserialize(null, keyBytes);
        assertEquals("correlation-id", key);
    }
    @Test
    public void testExtractKey_keyPropertyNoCorrelationId() throws Exception {
        KafkaTopicExchange kte = defaultExchange(cfg ->
                cfg.keyProperty("my-key"));

        KafkaExchangeInterpreter kafkaExchangeInterpreter = new KafkaExchangeInterpreter(bridgeConfig);
        CoreMessage message = new CoreMessage(1L, "body".length() + 50);
        message.putStringProperty("my-key", "super-keytastic!");
        message.setAddress("address");
        message.setDurable(true);
        message.setType(Message.BYTES_TYPE);
        message.setMessageID(1L);

        message.getBodyBuffer().writeString("body");

        byte[] keyBytes = kafkaExchangeInterpreter.extractKey(kte, message);
        String key = TestSupport.stringDeserializer().deserialize(null, keyBytes);
        assertEquals("super-keytastic!", key);
    }
}