/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import java.util.List;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface RoutingConfig {

  /**
   * <p>
   *  The list of topic routing rules that are used to establish JMS topic exchanges for passing
   *  data between top level Kafka topics and JMS topics.  Each rule can match any number of topics,
   *  including zero.
   * </p>
   * <p>
   *  Rules are applied in order and if multiple match a single topic the last applied rule will
   *  take affect.
   * </p>
   * @return a list of Kafka topic matching rules to be applied in order
   */
  List<RoutedTopic> topics();

  /**
   * <p>
   *  Config Key: <pre>metadata-refresh-ms</pre>
   * </p>
   * <p>
   *   Set the interval at which metadata on topics will be refreshed from the Kafka broker. This
   *   refresh will determine if any new matching Kafka topics have been found and establish JMS
   *   topic exchanges for them. It will also disable or remove any existing JMS topic exchanges
   *   that no longer have a corresponding Kafka topic.
   * </p>
   * <p>
   *   This refresh is affected by both changes in the existence of topics (create / delete) and
   *   access controls (read / write permission).
   * </p>
   * <p>
   *   DEFAULT: 300000, five minutes
   * </p>
   * @return The refresh interval as measured in milliseconds
   */
  Integer metadataRefreshMs();

  class Builder extends RoutingConfig_Builder {

    public Builder() {
      //defaults
      //5 minutes
      metadataRefreshMs(1000 * 60 * 5);
    }

    public Builder(Config routingConfig) {
      //set defaults
      this();

      if (routingConfig.hasPath("metadata-refresh-ms")) {
        metadataRefreshMs(routingConfig.getInt("metadata-refresh-ms"));
      }

      if (routingConfig.hasPath("topics")) {
        for (Config topicConfig : routingConfig.getConfigList("topics")) {
          this.addTopics(new RoutedTopic.Builder(topicConfig));
        }
      }


    }
  }

  @FreeBuilder
  interface RoutedTopic {

    /**
     * <p>
     * Config Key: <pre>match</pre>
     * </p>
     * <p>
     * A valid regular expression that will be used to match against topics available to the JMS
     * Bridge Kafka principle (as configured). Any topic that matches the expression will become
     * available via the JMS Bridge as an address and all data sent to that address will be routed
     * to Kafka using the configuration of this routed topic.
     * </p>
     * <p>
     * This match also applies to data being received from Kafka. All received data will be sent
     * to the address created by this routed topic and be made available as a JMS message via the
     * bridge.
     * </p>
     * <p>
     * This is REQUIRED.
     * </p>
     */
    String match();

    /**
     * <p>
     * Config Key: <pre>address-template</pre>
     * </p>
     * <p>
     * A simple character template that can be used to determine the name of the JMS topic
     * that will be created for each Kafka topic matching this. A single token is available for
     * replacement, '${topic}', it will be replaced by the name of the matched kafka topic.
     * </p>
     * <p>
     * By default this is set to 'kafka.${topic}';
     * </p>
     */
    String addressTemplate();

    /**
     * <p>
     * Config Key: <pre>message-type</pre>
     * </p>
     * <p>
     * For messages received from Kafka this will be the JMS message type used when deserializing
     * it. Valid options include 'TEXT' and 'BYTES'. Text assumes the data is UTF-8 encoded
     * characters while bytes is basically a pass-through of the payload.
     * </p>
     * <p>
     * By default this is set to 'BYTES'.
     * </p>
     */
    String messageType();

    /**
     * <p>
     * Config Key: <pre>key-property</pre>
     * </p>
     * <p>
     * Specify a property on the message that should be used as the record key when publishing
     * to Kafka. If the property cannot be found then the 'JMSMessageID' will be used.
     * </p>
     * <p>
     * By default this is set to 'JMSMessageID'.
     * </p>
     */
    String keyProperty();

    /**
     * <p>
     * Config Key: <pre>correlation-key-override</pre>
     * </p>
     * <p>
     * If this is set to true then when a correlation ID is found on the message it will be used
     * as the Kafka record key instead of the property set by the 'key-property'
     * ({@link #keyProperty()}) configuration.
     * </p>
     * <p>
     * By default this is set to 'true'.
     * </p>
     */
    boolean correlationKeyOverride();

    /**
     * <p>
     *   Config Key: <pre>consume-always</pre>
     * </p>
     * <p>
     *   Always consume and route messages from the matching Kafka topics regardless of whether
     *   anybody is listening (no bindings). This is required for situations where data may be
     *   routed to destinations that may not be bound to the corresponding JMS topic
     *   ( e.g. request/reply pattern with temprary queues).
     * </p>
     * <p>
     *   Normally, to conserver resources, if a topic exchange is not bound (no queue or consumer
     *   attached to the JMS topic) then no data will be read from the corresponding Kafka topic
     *   since it will not be routed.
     * </p>
     * <p>
     *   DEFAULT: false, save resources by not consuming from Kafka
     * </p>
     */
    boolean consumeAlways();

    /**
     * <p>
     *    Config Key: <pre>resume-at-latest</pre>
     * </p>
     * <p>
     *   If a topic exchange becomes inactive (no bindings attached to the JMS topic) then later
     *   is bound and becomes active again the underlying consumer will start reading data from
     *   Kafka using the previous active point offset. If the previous active point offset is no
     *   longer present in Kafka for that topic (due to expiration) then it will start reading
     *   from the head of the Kafka topic (latest offset).
     * </p>
     * <p>
     *   This option can be used to force reading, upon reactivation of the topic exchange, from
     *   the head of the Kafka topic instead of from any previous active point offset.
     * </p>
     * <p>
     *   <b>THIS FEATURE IS NOT CURRENTLY AVAILABLE</b>
     * </p>
     * <p>
     *   DEFAULT: false, use any previous active point offset when resuming
     * </p>
     */
    boolean resumeAtLatest();

    class Builder extends RoutingConfig_RoutedTopic_Builder {

      public Builder(Config routedTopicConfig) {
        //set defaults
        this();

        if (routedTopicConfig.hasPath("match")) {
          match(routedTopicConfig.getString("match"));
        }

        if (routedTopicConfig.hasPath("address-template")) {
          addressTemplate(routedTopicConfig.getString("address-template"));
        }

        if (routedTopicConfig.hasPath("message-type")) {
          messageType(routedTopicConfig.getString("message-type"));
        }

        if (routedTopicConfig.hasPath("key-property")) {
          keyProperty(routedTopicConfig.getString("key-property"));
        }

        if (routedTopicConfig.hasPath("correlation-key-override")) {
          correlationKeyOverride(routedTopicConfig.getBoolean("correlation-key-override"));
        }

        if (routedTopicConfig.hasPath("consume-always")) {
          consumeAlways(routedTopicConfig.getBoolean("consume-always"));
        }

        if (routedTopicConfig.hasPath("resume-at-latest")) {
          resumeAtLatest(routedTopicConfig.getBoolean("resume-at-latest"));
        }
      }

      public Builder() {
        //set defaults
        this.addressTemplate("kafka.${topic}")
            .messageType("BYTES")
            .keyProperty("JMSMessageID")
            .correlationKeyOverride(true)
            .consumeAlways(false)
            .resumeAtLatest(false);
      }

      @Override
      public Builder messageType(String messageType) {
        super.messageType(messageType);
        if (!messageType.equalsIgnoreCase("BYTES")
            && !messageType.equalsIgnoreCase("TEXT")) {
          throw new IllegalStateException(String.format(
              "Invalid message-type %s, must be either BYTES or TEXT.", messageType));
        }

        return this;
      }
    }
  }
}
