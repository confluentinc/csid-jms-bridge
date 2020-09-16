/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

public final class Headers {

  private Headers() {
  }

  private static final LongSerializer LONG_SERIALIZER = new LongSerializer();
  private static final IntegerSerializer INT_SERIALIZER = new IntegerSerializer();
  private static final StringSerializer STR_SERIALIZER = new StringSerializer();

  public static final String KNOWN_MSG_KEY_FORMAT = "jms.bridge.known.%s";
  public static final String JMS_KEY_PREFIX = "jms.";
  public static final String JMS_KEY_FORMAT = JMS_KEY_PREFIX + "%s";

  public static boolean isKnown(ICoreMessage message, String bridgeId) {
    return message.getPropertyNames().contains(
        SimpleString.toSimpleString(String.format(KNOWN_MSG_KEY_FORMAT, bridgeId)));
  }

  public static void markAsKnown(ICoreMessage message, String bridgeId) {
    MessageUtil.setStringProperty(message, String.format(KNOWN_MSG_KEY_FORMAT, bridgeId), "");
  }

  public static byte[] toBytes(Long l) {
    return LONG_SERIALIZER.serialize("", l);
  }

  public static byte[] toBytes(Integer i) {
    return INT_SERIALIZER.serialize("", i);
  }

  public static byte[] toBytes(String s) {
    return STR_SERIALIZER.serialize("", s);
  }

  public static Map<String, Object> getMessageProperties(
      ICoreMessage message) {

    Map<String, Object> propMap = new HashMap<>();
    propMap.put("JMSDestination", message.getAddress());
    propMap.put("JMSMessageID", message.getMessageID());
    propMap.put("JMSType", messageType(message.getType()));
    propMap.put("JMSTimestamp", message.getTimestamp());
    //propMap.put("JMSCorrelationID", MessageUtil.getJMSCorrelationID(message));
    //propMap.put("JMSReplyTo", MessageUtil.getJMSReplyTo(message));

    for (SimpleString hdrnamess : message.getPropertyNames()) {

      //don't translate headers we added
      String hdrname = hdrnamess.toString();

      //properties prefixed with '_' are internal properties
      if (!hdrname.startsWith(JMS_KEY_PREFIX) && !hdrname.startsWith("_")) {

        Object propVal = MessageUtil.getObjectProperty(message, hdrname);
        if (propVal != null) {
          propMap.put(hdrname, MessageUtil.getObjectProperty(message, hdrname));
        }
      }
    }
    return propMap;
  }

  public static Map<String, byte[]> convertHeaders(
      ICoreMessage message, String bridgeId, boolean addKnownMarker) {

    final Map<String, byte[]> headerMap = new HashMap<>();
    if (addKnownMarker) {
      headerMap.put(
          String.format(KNOWN_MSG_KEY_FORMAT, bridgeId),
          STR_SERIALIZER.serialize("", ""));
    }

    getMessageProperties(message).forEach((k, v) ->
        objectToBytes(v).ifPresent(vb -> headerMap.put(String.format(JMS_KEY_FORMAT, k), vb)));

    return headerMap;
  }

  public static Optional<byte[]> objectToBytes(Object subject) {
    byte[] result = null;
    if (subject != null) {
      if (subject instanceof byte[]) {
        result = (byte[]) subject;
      } else if (subject instanceof Long) {
        result = LONG_SERIALIZER.serialize("", (Long) subject);
      } else if (subject instanceof Integer) {
        result = INT_SERIALIZER.serialize("", (Integer) subject);
      } else if (subject instanceof String) {
        result = STR_SERIALIZER.serialize("", (String) subject);
      }
    }
    return Optional.ofNullable(result);
  }

  public static String messageType(byte type) {
    switch (type) {
      case Message.BYTES_TYPE:
        return "bytes";
      case Message.TEXT_TYPE:
        return "text";
      case Message.MAP_TYPE:
        return "map";
      case Message.STREAM_TYPE:
        return "stream";
      case Message.OBJECT_TYPE:
        return "object";
      case Message.EMBEDDED_TYPE:
        return "embedded";
      default:
        return "other";
    }
  }
}
