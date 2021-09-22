/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.exchange;

import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.activemq.artemis.utils.UUID;
import org.apache.activemq.artemis.utils.UUIDGenerator;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.record.Record;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.jupiter.api.Assertions.*;

class HeadersTest {

  @Test
  void testConvertJmsHeaders() {
    long ts = System.currentTimeMillis();
    UUID userId = UUIDGenerator.getInstance().generateUUID();

    CoreMessage message = new CoreMessage(1L, "body".length() + 50);
    message.setAddress("address");
    message.setRoutingType(RoutingType.MULTICAST);
    message.setDurable(true);
    message.setType(Message.BYTES_TYPE);
    message.setTimestamp(ts);
    message.setCorrelationID("correlation-id");
    message.setMessageID(1L);
    message.setReplyTo(SimpleString.toSimpleString("reply-to"));
    message.setUserID(userId);
    message.setAnnotation(SimpleString.toSimpleString("cust-prop-key"), "cust-prop-val");
    message.setBrokerProperty(SimpleString.toSimpleString("broker-prop-key"), "broker-prop-val");
    message.putStringProperty("string-property", "string-property-val");
    message.putIntProperty("int-property", 1);
    message.putLongProperty("long-property", 2L);
    message.putBytesProperty("bytes-property", "bytes-property-val".getBytes(StandardCharsets.UTF_8));
    message.putBooleanProperty("boolean-property", true);
    message.putCharProperty("char-property", 'a');
    message.putDoubleProperty("double-property", 1.99D);
    message.putShortProperty("short-property", (short)3);
    message.putFloatProperty("float-property", 0.99F);
    message.putObjectProperty("object-property", "object-property");

    message.getBodyBuffer().writeString("body");

    Map<String, byte[]> mappedHeaders =
        Headers.convertHeaders(message, "bridge-id", true);
    assertThat(mappedHeaders).containsKey("jms.string.JMSReplyTo");
    assertThat(mappedHeaders).containsKey("jms.long.JMSTimestamp");
    assertThat(mappedHeaders).containsKey("jms.string.JMSType");
    assertThat(mappedHeaders).containsKey("jmsbridge_bridge_id_hops");
    assertThat(mappedHeaders).containsKey("jms.long.JMSMessageID");
    assertThat(mappedHeaders).containsKey("jms.string.JMSCorrelationID");
    assertThat(mappedHeaders).containsKey("jms.string.cust-prop-key");
    assertThat(mappedHeaders).containsKey("jms.string.broker-prop-key");
    assertThat(mappedHeaders).containsKey("jms.string.string-property");
    assertThat(mappedHeaders).containsKey("jms.int.int-property");
    assertThat(mappedHeaders).containsKey("jms.long.long-property");
    assertThat(mappedHeaders).containsKey("jms.bytes.bytes-property");
    assertThat(mappedHeaders).containsKey("jms.string.object-property");

    //Currently not support types
    //assertThat(mappedHeaders).containsKey("jms.boolean.boolean-property");
    //assertThat(mappedHeaders).containsKey("jms.char.char-property");
    //assertThat(mappedHeaders).containsKey("jms.double.double-property");
    //assertThat(mappedHeaders).containsKey("jms.short.short-property");
    //assertThat(mappedHeaders).containsKey("jms.float.float-property");
  }

  @Test
  void testConvertKafkaHeaders() {
    long ts = System.currentTimeMillis();
    UUID userId = UUIDGenerator.getInstance().generateUUID();

    CoreMessage message = new CoreMessage(1L, "body".length() + 50);
    message.setAddress("address");
    message.setRoutingType(RoutingType.MULTICAST);
    message.setDurable(true);
    message.setType(Message.BYTES_TYPE);
    message.setTimestamp(ts);
    message.setCorrelationID("correlation-id");
    message.setMessageID(1L);
    message.setReplyTo(SimpleString.toSimpleString("reply-to"));
    message.setUserID(userId);
    message.setAnnotation(SimpleString.toSimpleString("cust-prop-key"), "cust-prop-val");
    message.setBrokerProperty(SimpleString.toSimpleString("broker-prop-key"), "broker-prop-val");
    message.putStringProperty("string-property", "string-property-val");
    message.putIntProperty("int-property", 1);
    message.putLongProperty("long-property", 2L);
    message.putBytesProperty("bytes-property", "bytes-property-val".getBytes(StandardCharsets.UTF_8));
    message.putBooleanProperty("boolean-property", true);
    message.putCharProperty("char-property", 'a');
    message.putDoubleProperty("double-property", 1.99D);
    message.putShortProperty("short-property", (short)3);
    message.putFloatProperty("float-property", 0.99F);
    message.putObjectProperty("object-property", "object-property");

    Map<String, byte[]> jmsHeaders =
        Headers.convertHeaders(message, "bridge-id", true);
    ProducerRecord<?, ?>  krecord = new ProducerRecord<>("topic", "body");
    jmsHeaders.forEach((k, v) -> krecord.headers().add(k, v));

    Map<String, Object> mappedHeaders = Headers.convertHeaders(
        krecord.headers(), "bridge-id", true);

    assertThat(mappedHeaders).containsKey("JMSReplyTo");
    assertThat(mappedHeaders.get("JMSReplyTo")).isInstanceOf(String.class);

    assertThat(mappedHeaders).containsKey("JMSTimestamp");
    assertThat(mappedHeaders.get("JMSTimestamp")).isInstanceOf(Long.class);

    assertThat(mappedHeaders).containsKey("JMSType");
    assertThat(mappedHeaders.get("JMSReplyTo")).isInstanceOf(String.class);

    assertThat(mappedHeaders).containsKey("jmsbridge_bridge_id_hops");

    assertThat(mappedHeaders).containsKey("JMSMessageID");
    assertThat(mappedHeaders.get("JMSMessageID")).isInstanceOf(Long.class);

    assertThat(mappedHeaders).containsKey("JMSCorrelationID");
    assertThat(mappedHeaders.get("JMSCorrelationID")).isInstanceOf(String.class);

    assertThat(mappedHeaders).containsKey("cust-prop-key");
    assertThat(mappedHeaders.get("cust-prop-key")).isInstanceOf(String.class);

    assertThat(mappedHeaders).containsKey("broker-prop-key");
    assertThat(mappedHeaders.get("broker-prop-key")).isInstanceOf(String.class);

    assertThat(mappedHeaders).containsKey("string-property");
    assertThat(mappedHeaders.get("string-property")).isInstanceOf(String.class);

    assertThat(mappedHeaders).containsKey("int-property");
    assertThat(mappedHeaders.get("int-property")).isInstanceOf(Integer.class);

    assertThat(mappedHeaders).containsKey("long-property");
    assertThat(mappedHeaders.get("long-property")).isInstanceOf(Long.class);

    assertThat(mappedHeaders).containsKey("bytes-property");
    assertThat(mappedHeaders.get("bytes-property")).isInstanceOf(byte[].class);

    assertThat(mappedHeaders).containsKey("object-property");
    assertThat(mappedHeaders.get("object-property")).isInstanceOf(String.class);

    //Currently not support types
    //assertThat(mappedHeaders).containsKey("jms.boolean.boolean-property");
    //assertThat(mappedHeaders).containsKey("jms.char.char-property");
    //assertThat(mappedHeaders).containsKey("jms.double.double-property");
    //assertThat(mappedHeaders).containsKey("jms.short.short-property");
    //assertThat(mappedHeaders).containsKey("jms.float.float-property");
  }


}