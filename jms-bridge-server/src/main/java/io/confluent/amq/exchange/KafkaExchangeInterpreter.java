/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.logging.StructuredLogger;
import org.apache.activemq.artemis.api.core.ActiveMQQueueExistsException;
import org.apache.activemq.artemis.api.core.ICoreMessage;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;

import java.util.EnumSet;

/**
 * Implements behavior that can be applied against {@link KafkaExchange} value instances.
 */
public class KafkaExchangeInterpreter {
  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeInterpreter.class));

  private static final EnumSet<KExMessageType> ROUTEABLE_MESSAGE_TYPES = EnumSet.of(
      KExMessageType.BYTES,
      KExMessageType.TEXT
  );

  private final BridgeConfig bridgeConfig;

  public KafkaExchangeInterpreter(BridgeConfig bridgeConfig) {
    this.bridgeConfig = bridgeConfig;
  }


  public boolean isRoutable(ICoreMessage message) {

    boolean result =  ROUTEABLE_MESSAGE_TYPES.contains(KExMessageType.fromId(message.getType()))
        && Headers.getHopsValue(message, bridgeConfig.id()) < 1;

    return result;
  }

  /**
   * Creates and registeres the AMQ entities (Address, Queue, Divert) required for a topic exchange
   * to function fully. If the entities already exist then they will be updated if required and
   * returned.
   *
   * @return the AMQ entities created or found
   */
  public IngressEntities createExchangeEntities(
      KafkaTopicExchange exchange, ConfluentAmqServer amqServer) {

    try {
      final AddressInfo topicAddress = createIngressTopic(exchange.amqAddressName(), amqServer);

      Queue ingressQueue = createIngressQueue(
          exchange.ingressQueueName(), exchange.ingressQueueName(), amqServer);
      return new IngressEntities(topicAddress, ingressQueue);
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("CreateExchangeEntities")
          .markFailure()
          .putTokens("exchange", exchange)
          .message("Failed to create/update entity Queue or Address for exchange."), e);
      throw new RuntimeException(e);
    }
  }


  private AddressInfo createIngressTopic(
      String name, ConfluentAmqServer amqServer) throws Exception {

    AddressInfo ingressAddress = new AddressInfo(
        SimpleString.toSimpleString(name),
        RoutingType.MULTICAST);

    return amqServer.addOrUpdateAddressInfo(ingressAddress);
  }

  private Queue createIngressQueue(
      String addressName, String name, ConfluentAmqServer amqServer) throws Exception {

    QueueConfiguration ingressQueueConf = new QueueConfiguration(name)
        .setAutoDelete(false)
        .setInternal(true)
        .setAddress(addressName)
        .setRoutingType(RoutingType.MULTICAST)
        .setDurable(true);

    Queue queue;
    try {
      queue = amqServer.createQueue(ingressQueueConf);
      SLOG.info(b -> b
          .event("CreateIngressQueue")
          .name(name));
    } catch (ActiveMQQueueExistsException e) {
      queue = amqServer.updateQueue(ingressQueueConf);
      SLOG.info(b -> b
          .event("UpdateIngressQueue")
          .name(name));
    }
    return queue;
  }


  public byte[] extractKey(KafkaTopicExchange exchange, Message message) {
    byte[] key = null;
    if (exchange.originConfig().correlationKeyOverride() && message.getCorrelationID() != null) {
      key = Headers
          .objectToBytes(message.getCorrelationID())
          .map(Headers.TypedHeaderValue::getValue)
          .orElse(null);
    }

    if (null == key && message.getObjectProperty(exchange.originConfig().keyProperty()) != null) {
      key = Headers
          .objectToBytes(message.getObjectProperty(exchange.originConfig().keyProperty()))
          .map(Headers.TypedHeaderValue::getValue)
          .orElse(null);
    }

    if (null == key) {
      key = Headers
          .objectToBytes(message.getMessageID())
          .map(Headers.TypedHeaderValue::getValue)
          .orElse(null);
    }

    return key;
  }

  public void deleteIngressEntities(
      KafkaTopicExchange kte, ConfluentAmqServer amqServer) throws Exception {

    int bindings = amqServer.getPostOffice()
        .getBindingsForAddress(SimpleString.toSimpleString(kte.amqAddressName()))
        .getBindings()
        .size();

    if (bindings > 1) {
      //pause the address allowing existing bindings to stay valid.
      AddressControl topicAddressControl =
          (AddressControl) amqServer.getManagementService().getResource(
              ResourceNames.ADDRESS + kte.amqAddressName());
      topicAddressControl.pause(true);

    } else {
      //only the divert remains bound
      Queue ingressQueue = amqServer.locateQueue(kte.ingressQueueName());
      if (ingressQueue != null) {
        ingressQueue.deleteQueue(true);
      }

      amqServer.removeAddressInfo(SimpleString.toSimpleString(kte.amqAddressName()), null, true);
    }
  }


  public static class IngressEntities {

    private final AddressInfo topicAddress;
    private final Queue ingressQueue;

    IngressEntities(
        AddressInfo topicAddress,
        Queue ingressQueue) {

      this.topicAddress = topicAddress;
      this.ingressQueue = ingressQueue;
    }

    public AddressInfo getTopicAddress() {
      return topicAddress;
    }

    public Queue getIngressQueue() {
      return ingressQueue;
    }
  }
}
