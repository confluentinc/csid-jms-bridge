/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.artemis.api.core.ICoreMessage;
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

  public static Map<String, byte[]> convertHeaders(
      ICoreMessage message, String bridgeId, boolean addKnownMarker) {

    final Map<String, byte[]> headerMap = new HashMap<>();
    if (addKnownMarker) {
      headerMap.put(
          String.format(KNOWN_MSG_KEY_FORMAT, bridgeId),
          STR_SERIALIZER.serialize("", ""));
    }

    for (String hdrname : MessageUtil.getPropertyNames(message)) {
      //don't translate headers we added
      if (!hdrname.startsWith(JMS_KEY_PREFIX)) {
        Object property = MessageUtil.getObjectProperty(message, hdrname);
        byte[] propdata = objectToBytes(property);

        if (propdata != null) {
          headerMap.put(String.format(JMS_KEY_FORMAT, hdrname), propdata);
        }
      }
    }
    return headerMap;
  }

  public static byte[] objectToBytes(Object subject) {
    if (subject != null) {
      if (subject instanceof byte[]) {
        return (byte[]) subject;
      } else if (subject instanceof Long) {
        return LONG_SERIALIZER.serialize("", (Long) subject);
      } else if (subject instanceof Integer) {
        return INT_SERIALIZER.serialize("", (Integer) subject);
      } else if (subject instanceof String) {
        return STR_SERIALIZER.serialize("", (String) subject);
      }
    }
    return null;
  }
}
