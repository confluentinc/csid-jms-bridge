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
import org.junit.jupiter.api.Test;

import java.util.Map;

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
    message.getBodyBuffer().writeString("body");

    Map<String, byte[]> mappedHeaders =
        Headers.convertHeaders(message, "bridge-id", true);
    System.out.println(mappedHeaders);
  }
}