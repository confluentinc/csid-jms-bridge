/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.reader.MessageUtil;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import io.confluent.amq.logging.StructuredLogger;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public final class Headers {

  public static final String HDR_CORRELATION_ID_KEY = "JMSCorrelationID";
  public static final String HDR_REPLY_TO_KEY = "JMSReplyTo";
  public static final TypedHeader HDR_MESSAGE_ID =
      new TypedHeader("JMSMessageID", HeaderType.LONG);
  public static final TypedHeader HDR_CORRELATION_ID =
      new TypedHeader(HDR_CORRELATION_ID_KEY, HeaderType.STRING);
  public static final TypedHeader HDR_DESTINATION =
      new TypedHeader("JMSDestination", HeaderType.STRING);
  public static final TypedHeader HDR_TYPE =
      new TypedHeader("JMSType", HeaderType.STRING);
  public static final TypedHeader HDR_TIMESTAMP =
      new TypedHeader("JMSTimestamp", HeaderType.LONG);
  public static final TypedHeader HDR_REPLY_TO =
      new TypedHeader(HDR_REPLY_TO_KEY, HeaderType.STRING);
  public static final String HDR_KAFKA_TOPIC = "kafka.topic";
  public static final String HDR_KAFKA_PARTITION = "kafka.partition";
  public static final String HDR_KAFKA_OFFSET = "kafka.offset";
  public static final String HDR_KAFKA_KEY = "kafka.key";


  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(Headers.class));

  private Headers() {
  }

  private static final Serde<byte[]> BYTES_SERDES = Serdes.ByteArray();
  private static final Serde<Long> LONG_SERDES = Serdes.Long();
  private static final Serde<Integer> INT_SERDES = Serdes.Integer();
  private static final Serde<String> STR_SERDES = Serdes.String();

  private static final String HOPS_MSG_KEY_FORMAT = "jmsbridge_%s_hops";
  private static final String JMS_KEY_PREFIX = "jms.";
  private static final String JMS_KEY_FORMAT = JMS_KEY_PREFIX + "%s";
  private static final String JMS_KEY_TYPED_FORMAT = JMS_KEY_PREFIX + "%s.%s";

  /**
   * Translates a JMS/AMQ message property/header key into the equivalent Kafka record header key.
   *
   * @param jmsProp the JMS/AMQ property/header key
   * @return kafka header key
   */
  public static String createKafkaJmsPropKey(String jmsProp, HeaderType type) {
    if (type != null && type != HeaderType.UNKNOWN) {
      return String.format(JMS_KEY_TYPED_FORMAT, type.getCode(), jmsProp);
    }
    return String.format(JMS_KEY_FORMAT, jmsProp);
  }

  public static String createKafkaJmsPropKey(TypedHeader jmsProp) {
    return createKafkaJmsPropKey(jmsProp.getHeaderKey(), jmsProp.getType());
  }

  public static String createKafkaJmsPropKey(String untypedJmsProp) {
    return createKafkaJmsPropKey(untypedJmsProp, null);
  }

  /**
   * The hops value is used to short circuit data cycles between the JMS Bridge and Kafka. This
   * method generates that key which is the same for both AMQ and Kafka records.
   *
   * @param bridgeId The id of the bridge in which the hops header applies to
   * @return the hops header key for both Kafka and AMQ records
   */
  public static String createHopsKey(String bridgeId) {
    return String.format(HOPS_MSG_KEY_FORMAT, bridgeId).replace(".", "_").replace("-", "_");
  }

  /**
   * Lookups the header by key and if found returns it as a byte[], otherwise it will be empty.
   *
   * @param hdrName the name of the header to lookup
   * @param headers the headers object from the Kafka record
   * @return the value of the header if it exists otherwise empty
   */
  public static Optional<byte[]> getBytesHeader(
      String hdrName,
      org.apache.kafka.common.header.Headers headers) {

    return getHeader(hdrName, headers, BYTES_SERDES.deserializer());
  }


  /**
   * Lookups the header by key and if found returns it as a String, otherwise it will be empty.
   *
   * @param hdrName the name of the header to lookup
   * @param headers the headers object from the Kafka record
   * @return the value of the header if it exists otherwise empty
   * @throws this method my throw a {@link org.apache.kafka.common.errors.SerializationException}
   */
  public static Optional<String> getStringHeader(
      String hdrName,
      org.apache.kafka.common.header.Headers headers) {

    return getHeader(hdrName, headers, STR_SERDES.deserializer());
  }

  /**
   * Lookups the header by key and if found returns it as a Long, otherwise it will be empty.
   *
   * @param hdrName the name of the header to lookup
   * @param headers the headers object from the Kafka record
   * @return the value of the header if it exists otherwise empty
   * @throws this method my throw a {@link org.apache.kafka.common.errors.SerializationException}
   */
  public static Optional<Long> getLongHeader(
      String hdrName,
      org.apache.kafka.common.header.Headers headers) {

    return getHeader(hdrName, headers, LONG_SERDES.deserializer());
  }

  /**
   * Lookups the header by key and if found returns it as a Integer, otherwise it will be empty.
   *
   * @param hdrName the name of the header to lookup
   * @param headers the headers object from the Kafka record
   * @return the value of the header if it exists otherwise empty
   * @throws this method my throw a {@link org.apache.kafka.common.errors.SerializationException}
   */
  public static Optional<Integer> getIntHeader(
      String hdrName,
      org.apache.kafka.common.header.Headers headers) {

    return getHeader(hdrName, headers, INT_SERDES.deserializer());
  }

  private static <T> Optional<T> getHeader(
      String hdrName,
      org.apache.kafka.common.header.Headers headers,
      Deserializer<T> deserializer) {

    if (headers == null) {
      return Optional.empty();
    }

    Header hdr = headers.lastHeader(hdrName);
    if (hdr == null) {
      return Optional.empty();
    }

    return Optional.ofNullable(deserializer.deserialize("", hdr.value()));

  }

  /**
   * Serialize a Long to a byte[].
   *
   * @param l the Long to serialize
   * @return the byte[] equivelant
   */
  public static byte[] toBytes(Long l) {
    return LONG_SERDES.serializer().serialize("", l);
  }

  /**
   * Serialize an Integer to a byte[].
   *
   * @param i the Integer to serialize
   * @return the byte[] equivelant
   */
  public static byte[] toBytes(Integer i) {
    return INT_SERDES.serializer().serialize("", i);
  }

  /**
   * Serialize a String to a byte[].
   *
   * @param s the String to serialize
   * @return the byte[] equivelant
   */
  public static byte[] toBytes(String s) {
    return STR_SERDES.serializer().serialize("", s);
  }

  /**
   * Gathers all headers/properties found on the AMQ message. This also includes standard properties
   * not found as a message property but is part of the {@link ICoreMessage} such as {@link
   * ICoreMessage#getMessageID()} (JMSMessageID).
   *
   * @param message the message to extract the headers/properties from
   * @return a map of key to value retaining the value type.
   */
  public static Map<String, Object> getMessageProperties(
      ICoreMessage message) {

    Map<String, Object> propMap = new HashMap<>();
    propMap.put(HDR_DESTINATION.getHeaderKey(), MessageUtil.getObjectProperty(
        message, Message.HDR_ORIGINAL_ADDRESS.toString()));
    propMap.put(HDR_MESSAGE_ID.getHeaderKey(), message.getMessageID());
    propMap.put(HDR_TYPE.getHeaderKey(), messageType(message.getType()));
    propMap.put(HDR_TIMESTAMP.getHeaderKey(), message.getTimestamp());

    for (SimpleString hdrnamess : message.getPropertyNames()) {

      //don't translate headers we added
      String hdrname = hdrnamess.toString();

      //properties prefixed with '_' are internal properties
      if (!hdrname.startsWith("_")) {

        Object propVal = MessageUtil.getObjectProperty(message, hdrname);
        if (propVal != null) {
          propMap.put(hdrname, MessageUtil.getObjectProperty(message, hdrname));
        }
      }
    }
    return propMap;
  }

  /**
   * Converts all JMS prefixed headers found on a Kafka record to key/value pairs that can be used
   * to populate  an AMQ {@link Message}. Additionally it can increment the hops header, if the
   * header is not present it will assume a value of 0 and increment that then add it to the header
   * map. Note that the JMS prefix will be stripped.
   *
   * @param headers       the Kafka record {@link Header} object
   * @param bridgeId      the id of the JMS Bridge
   * @param incrementHops whether to increment a hops header, will create one if not present
   * @return a map of headers suitable for adding to an AMQ message
   */
  @SuppressWarnings("checkstyle:CyclomaticComplexity")
  public static Map<String, Object> convertHeaders(
      org.apache.kafka.common.header.Headers headers,
      String bridgeId,
      boolean incrementHops) {

    final Map<String, Object> headerMap = new HashMap<>();

    if (incrementHops) {
      String hopsKey = createHopsKey(bridgeId);
      int hopsVal = getIntHeader(hopsKey, headers)
          .map(hops -> hops + 1)
          .orElse(1);
      headerMap.put(hopsKey, hopsVal);
    }

    if (headers == null) {
      return headerMap;
    }

    headers.forEach(hdr -> {
      String hdrKey = hdr.key();
      if (hdrKey.startsWith(JMS_KEY_PREFIX)) {
        String remaining = hdrKey.substring(JMS_KEY_PREFIX.length());

        final HeaderType hdrType = extractHeaderType(remaining);
        if (HeaderType.UNKNOWN != hdrType) {
          remaining = remaining.substring(hdrType.code.length() + 1);
        }

        String jmsKey = remaining;
        switch (jmsKey) {
          case HDR_CORRELATION_ID_KEY:
          case HDR_REPLY_TO_KEY:
            headerMap.put(jmsKey, STR_SERDES.deserializer().deserialize("", hdr.value()));
            break;
          default:
            switch (hdrType) {
              case STRING:
                headerMap.put(jmsKey, STR_SERDES.deserializer().deserialize("", hdr.value()));
                break;
              case LONG:
                headerMap.put(jmsKey, LONG_SERDES.deserializer().deserialize("", hdr.value()));
                break;
              case INT:
                headerMap.put(jmsKey, INT_SERDES.deserializer().deserialize("", hdr.value()));
                break;
              case BYTES:
                headerMap.put(jmsKey, hdr.value());
                break;
              default:
                SLOG.debug(b -> b
                    .event("ConvertHeaders")
                    .message("Unsupported/Unknown header type found, defaulting to 'bytes'.")
                    .putTokens("headerKey", hdrKey)
                    .putTokens("headerType", hdrType.toString()));
                headerMap.put(jmsKey, hdr.value());
                break;
            }
            break;
        }
      }
    });

    return headerMap;
  }

  /**
   * Converts all headers found on an AMQ message to key/value pairs that can be used to populate a
   * Kafka record header object {@link Header}. Additionally it can increment the hops header, if
   * the header is not present it will assume a value of 0 and increment that then add it to the
   * header map. Note it will add the JMS prefix to each key if it is already not prefixed.
   *
   * @param message       the AMQ message
   * @param bridgeId      the id of the JMS Bridge
   * @param incrementHops whether to increment a hops header, will create one if not present
   * @return a map of headers suitable for adding to a Kafka record
   */
  public static Map<String, byte[]> convertHeaders(
      ICoreMessage message, String bridgeId, boolean incrementHops) {

    final Map<String, byte[]> headerMap = new HashMap<>();

    getMessageProperties(message).forEach((k, v) ->
        objectToBytes(v).ifPresent(vb -> {
          if (k.startsWith(JMS_KEY_PREFIX)) {
            headerMap.put(k, vb.getValue());
          } else {
            headerMap.put(createKafkaJmsPropKey(k, vb.getType()), vb.getValue());
          }
        }));

    if (incrementHops) {
      String hopsKey = createHopsKey(bridgeId);
      int hops = 1;
      if (headerMap.containsKey(hopsKey)) {
        byte[] hopsBytes = headerMap.get(hopsKey);
        try {
          hops = INT_SERDES.deserializer().deserialize("", hopsBytes);
        } catch (Exception e) {
          SLOG.warn(b -> b
              .event("ExtractHopsHeader")
              .markFailure()
              .message("Failed to deserialize hops header, will reset to 1."), e);
        }
        hops = hops + 1;
      }

      headerMap.put(hopsKey, toBytes(hops));
    }

    return headerMap;
  }

  private static HeaderType extractHeaderType(String hdrWithoutJmsPrefix) {
    int dotIdx = hdrWithoutJmsPrefix.indexOf('.');

    HeaderType hdrType = HeaderType.UNKNOWN;
    if (dotIdx != -1) {
      hdrType = HeaderType.fromCode(hdrWithoutJmsPrefix.substring(0, dotIdx));
    }
    return hdrType;
  }

  /**
   * Interrogates the given subject Object and attempts to serialize it into a byte[]. If it cannot
   * be serialized then empty is returned.
   * <p>
   * Currently only supports Long, Integer, byte[] and String.
   * </p>
   *
   * @param subject the object to serialize
   * @return the serialized object or empty if it cannot be serialized.
   */
  public static Optional<TypedHeaderValue> objectToBytes(Object subject) {
    byte[] result = null;
    HeaderType type = HeaderType.UNKNOWN;
    if (subject != null) {
      if (subject instanceof byte[]) {
        result = (byte[]) subject;
        type = HeaderType.BYTES;
      } else if (subject instanceof Long) {
        result = LONG_SERDES.serializer().serialize("", (Long) subject);
        type = HeaderType.LONG;
      } else if (subject instanceof Integer) {
        result = INT_SERDES.serializer().serialize("", (Integer) subject);
        type = HeaderType.INT;
      } else if (subject instanceof String) {
        result = STR_SERDES.serializer().serialize("", (String) subject);
        type = HeaderType.STRING;
      }
    }
    if (result != null) {
      return Optional.of(new TypedHeaderValue(type, result));
    }

    return Optional.empty();
  }

  /**
   * Returns the textual representation for {@link ICoreMessage#getType()}.
   *
   * @param type the byte type indicator
   * @return textual representation of the type, based on {@link KExMessageType}
   */
  public static String messageType(byte type) {
    return KExMessageType.fromId(type).name();
  }

  public enum HeaderType {
    STRING("string"),
    INT("int"),
    LONG("long"),
    BYTES("bytes"),
    BOOLEAN("boolean"),
    CHAR("char"),
    FLOAT("float"),
    SHORT("short"),
    OBJECT("object"),
    UNKNOWN("unknown");

    public static HeaderType fromCode(String code) {
      try {
        return valueOf(code.toUpperCase());
      } catch (IllegalArgumentException e) {
        return UNKNOWN;
      }
    }

    private final String code;

    HeaderType(String code) {
      this.code = code;
    }

    public String getCode() {
      return code;
    }
  }

  public static class TypedHeader {

    private final String headerKey;
    private final HeaderType type;

    public TypedHeader(String headerKey, HeaderType type) {
      this.headerKey = headerKey;
      this.type = type;
    }

    public String getHeaderKey() {
      return headerKey;
    }

    public HeaderType getType() {
      return type;
    }
  }

  @SuppressFBWarnings({"EI_EXPOSE_REP2", "EI_EXPOSE_REP"})
  public static class TypedHeaderValue {

    private final HeaderType type;
    private final byte[] value;

    public TypedHeaderValue(HeaderType type, byte[] value) {
      this.type = type;
      this.value = value;
    }

    public HeaderType getType() {
      return type;
    }

    public byte[] getValue() {
      return value;
    }
  }
}
