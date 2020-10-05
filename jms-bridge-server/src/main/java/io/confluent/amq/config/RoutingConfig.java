/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import java.util.List;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface RoutingConfig {

  List<RoutedTopic> topics();

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
     * A simple character template that can be used to determine the name of the ActiveMQ address
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
     * to Kafka. If the property cannot be found then the 'MessageID' will be used.
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

      }

      public Builder() {
        //set defaults
        this.addressTemplate("kafka.${topic}")
            .messageType("BYTES")
            .keyProperty("JMSMessageID")
            .correlationKeyOverride(true);
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
