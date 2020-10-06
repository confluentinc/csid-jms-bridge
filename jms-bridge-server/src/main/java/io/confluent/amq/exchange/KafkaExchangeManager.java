/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import com.google.common.collect.Sets;
import io.confluent.amq.ComponentLifeCycle;
import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.JmsBridgeConfiguration;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.exchange.KafkaTopicExchange.Builder;
import io.confluent.amq.logging.StructuredLogger;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.api.core.management.DivertControl;
import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;

/**
 * Manages and maintains the active exchange of data between top level kafka topics and ActiveMQ
 * destinations.
 *
 * <p>
 * The manager will detect when either kafka or the bridge has changes that impacts the exchange.
 * When an impact does occur it will update the exchange to represent the new state.
 * </p>
 *
 * <p>
 * Potential changes that could impact the exchange are:
 * </p>
 *
 * <p>
 * 1. Kafka security changes 2. Kafka topic creation/deletion 3. AMQ address binding changes
 * </p>
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaExchangeManager implements ActiveMQServerPlugin {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeManager.class));

  private final JmsBridgeConfiguration bridgeConfig;
  private final ConfluentAmqServer amqServer;
  private final KafkaExchange exchange;
  private final KafkaExchangeIngress ingress;
  private final KafkaExchangeEgress egress;

  private final ComponentLifeCycle state = new ComponentLifeCycle(SLOG);

  public KafkaExchangeManager(JmsBridgeConfiguration bridgeConfig,
      ConfluentAmqServer amqServer) {

    this.bridgeConfig = bridgeConfig;
    this.amqServer = amqServer;

    this.exchange = new KafkaExchange();

    this.ingress = new KafkaExchangeIngress(
        bridgeConfig,
        this.exchange,
        amqServer.getKafkaIntegration().getKafkaIO(),
        Long.MAX_VALUE);

    this.egress = new KafkaExchangeEgress(
        this.bridgeConfig,
        this.amqServer,
        this.exchange,
        amqServer.getKafkaIntegration().getKafkaIO());
  }

  public boolean isEnabled() {
    return bridgeConfig.getBridgeConfig().routing().isPresent();
  }

  @Override
  public void init(Map<String, String> properties) {
    if (isEnabled()) {
      try {
        prepare();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      SLOG.info(b -> b
          .event("KafkaRoutingDisabled")
          .message("No routing configuration found."));
    }
  }

  public Collection<String> currentSubscribedKafkaTopics() {
    return this.egress.currentConsumerTopics();
  }

  public void start() {
    if (!isEnabled()) {
      return;
    }

    state.doStart(this::doStart);
  }

  private void doStart() {
    try {
      ingress.start();
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("Start")
          .markFailure()
          .message("Failed to start exchange ingress"), e);
      throw e;
    }
    try {
      egress.start();
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("Start")
          .markFailure()
          .message("Failed to start exchange egress"), e);
      throw e;
    }

    try {
      synchronizeTopics();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    RoutingConfig routingConfig = bridgeConfig.getBridgeConfig().routing().get();
    amqServer.getScheduledPool().schedule(() -> {
      if (state.isStarted()) {
        return;
      }

      try {
        this.synchronizeTopics();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }, routingConfig.metadataRefreshMs(), TimeUnit.MILLISECONDS);
  }

  public void stop() {
    if (!isEnabled()) {
      return;
    }

    state.doStop(this::doStop);
  }

  private void doStop() {
    try {
      ingress.stop();
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("Stop")
          .markFailure()
          .message("Failed to stop exchange ingress"), e);
    }
    try {
      egress.stop();
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("Stop")
          .markFailure()
          .message("Failed to stop exchange egress"), e);
    }
  }

  @Override
  public void afterAddBinding(Binding binding) throws ActiveMQException {
    this.exchange.bindingAdded(binding.getAddress().toString());
  }

  @Override
  public void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData)
      throws ActiveMQException {
    this.exchange.bindingRemoved(binding.getAddress().toString());
  }

  public void prepare() {
    state.doPrepare(() -> {
      //nothing
    });
  }

  public Queue createIngressQueue(String addressName, String name) throws Exception {
    QueueConfiguration ingressQueueConf = new QueueConfiguration(name)
        .setAddress(addressName)
        .setRoutingType(RoutingType.MULTICAST)
        .setDurable(true);
    Queue queue = amqServer.locateQueue(name);
    if (queue == null) {
      return amqServer.createQueue(ingressQueueConf);
    } else {
      return queue;
    }
  }

  public AddressInfo createIngressAddress(String name) throws Exception {
    AddressInfo ingressAddress = new AddressInfo(
        SimpleString.toSimpleString(name),
        RoutingType.MULTICAST);

    return amqServer.addOrUpdateAddressInfo(ingressAddress);
  }

  public void synchronizeTopics() throws Exception {
    synchronizeTopics(false);
  }

  public void synchronizeTopics(boolean forceCreate) throws Exception {
    SLOG.info(b -> b.event("SynchronizeTopics"));
    //create the basic ingress parts: address, queue and binding
    Set<KafkaTopicExchange> discoveredExchanges = discoverTopicExchanges();
    Set<KafkaTopicExchange> currentExchanges = exchange.getAllExchanges();
    Set<KafkaTopicExchange> newExchanges = Sets.difference(discoveredExchanges, currentExchanges);
    Set<KafkaTopicExchange> removedExchanges = Sets
        .difference(currentExchanges, discoveredExchanges);

    Map<KafkaTopicExchange, IngressEntities> kteEntityMap = null;
    if (forceCreate) {
      kteEntityMap = createExchangeEntities(discoveredExchanges);
    } else {
      kteEntityMap = createExchangeEntities(newExchanges);
    }

    for (KafkaTopicExchange kte : kteEntityMap.keySet()) {
      int bindings = amqServer.getPostOffice()
          .getBindingsForAddress(SimpleString.toSimpleString(kte.amqAddressName()))
          .getBindings()
          .size();

      exchange.addTopicExchange(kte, bindings);
    }

    for (KafkaTopicExchange kte : removedExchanges) {
      int bindings = amqServer.getPostOffice()
          .getBindingsForAddress(SimpleString.toSimpleString(kte.amqAddressName()))
          .getBindings()
          .size();
      if (bindings > 1) {
        exchange.pauseExchange(kte);
        AddressControl topicAddressControl =
            (AddressControl) amqServer.getManagementService().getResource(
                ResourceNames.ADDRESS + kte.amqAddressName());
        topicAddressControl.pause();
      } else {
        exchange.removeExchange(kte);
        deleteIngressEntities(kte);
      }
    }

    SLOG.info(b -> b.event("SynchronizeTopics").markCompleted());
  }

  public void deleteIngressEntities(KafkaTopicExchange kte) throws Exception {

    Queue ingressQueue = amqServer.locateQueue(kte.ingressQueueName());
    if (ingressQueue != null) {
      ingressQueue.deleteQueue(true);
    }

    amqServer.destroyDivert(SimpleString.toSimpleString(
        KafkaExchangeUtil.createDivertName(kte.amqAddressName())));
    amqServer.removeAddressInfo(SimpleString.toSimpleString(kte.ingressQueueName()), null, true);
    amqServer.removeAddressInfo(SimpleString.toSimpleString(kte.amqAddressName()), null, true);
  }

  @SuppressWarnings("OptionalGetWithoutIsPresent")
  public Set<KafkaTopicExchange> discoverTopicExchanges() {
    //go through the topics and match them against the routed topics creating topic exchanges
    RoutingConfig routingConfig = bridgeConfig.getBridgeConfig().routing().get();
    Set<String> kafkaTopics = amqServer.getKafkaIntegration().getKafkaIO().listTopics();
    Set<KafkaTopicExchange> allExchanges = new HashSet<>();
    for (RoutedTopic routedTopic : routingConfig.topics()) {
      SLOG.info(b -> b
          .event("ProcessRoutingTopicRule")
          .markStarted()
          .putTokens("topicRule", routedTopic));

      Pattern matcher = Pattern.compile(routedTopic.match());
      Set<KafkaTopicExchange> exchanges = kafkaTopics.stream()
          .filter(t -> !t.equals(amqServer.getKafkaIntegration().getBindingsJournal().topic()))
          .filter(t -> !t.equals(amqServer.getKafkaIntegration().getMessagesJournal().topic()))
          .filter(t -> !t.startsWith("_"))
          .filter(matcher.asPredicate())
          .map(topic -> new Builder()
              .originConfig(routedTopic)
              .amqAddressName(routedTopic.addressTemplate().replace("${topic}", topic))
              .kafkaTopicName(topic))
          .map(b -> b
              .ingressQueueName(KafkaExchangeUtil.createIngressDestinationName(b.amqAddressName()))
              .build())
          .collect(Collectors.toSet());

      allExchanges.forEach(kte -> kafkaTopics.remove(kte.kafkaTopicName()));
      SLOG.info(b -> b
          .event("ProcessRoutingTopicRule")
          .markCompleted()
          .putTokens("topicRule", routedTopic)
          .putTokens("topicMatchCount", exchanges.size())
          .putTokens("topicMatches", "["
              + exchanges.stream()
              .map(ex -> String.format(
                  "\"kafka://%s <--> jms://%s\"", ex.kafkaTopicName(), ex.amqAddressName()))
              .collect(Collectors.joining(", "))
              + "]"));
      allExchanges.addAll(exchanges);
    }
    return allExchanges;
  }

  public Map<KafkaTopicExchange, IngressEntities> createExchangeEntities(
      Set<KafkaTopicExchange> allExchanges) throws Exception {

    Map<KafkaTopicExchange, IngressEntities> resultMap = new HashMap<>();
    for (KafkaTopicExchange exchange : allExchanges) {

      AddressInfo topicAddress = new AddressInfo(
          SimpleString.toSimpleString(exchange.amqAddressName()),
          RoutingType.MULTICAST);
      topicAddress = amqServer.addOrUpdateAddressInfo(topicAddress);

      AddressInfo ingressAddress = new AddressInfo(
          SimpleString.toSimpleString(exchange.ingressQueueName()),
          RoutingType.MULTICAST);
      ingressAddress = amqServer.addOrUpdateAddressInfo(ingressAddress);

      String divertName = KafkaExchangeUtil.createDivertName(topicAddress.getName().toString());
      DivertConfiguration divertConfig = new DivertConfiguration()
          .setAddress(topicAddress.getName().toString())
          .setForwardingAddress(ingressAddress.getName().toString())
          .setExclusive(true)
          .setName(divertName)
          .setFilterString(divertFilter());
      DivertControl divertControl = (DivertControl) amqServer.getManagementService()
          .getResource(ResourceNames.DIVERT + divertName);

      if (divertControl == null) {
        amqServer.deployDivert(divertConfig);
      }

      Queue ingressQueue = createIngressQueue(
          exchange.ingressQueueName(), exchange.ingressQueueName());
      ingressQueue.addConsumer(this.ingress);

      resultMap.put(exchange, new IngressEntities(
          divertConfig.getName(), topicAddress, ingressAddress, ingressQueue));
    }

    return resultMap;
  }

  public String divertFilter() {
    String hopsHeaderKey = Headers.createHopsKey(bridgeConfig.getBridgeConfig().id());
    return String.format(" %s is null or %s < 1", hopsHeaderKey, hopsHeaderKey);
  }

  private static class IngressEntities {

    final String divertName;
    final AddressInfo topicAddress;
    final AddressInfo ingressAddress;
    final Queue ingressQueue;

    IngressEntities(String divertName,
        AddressInfo topicAddress,
        AddressInfo ingressAddress, Queue ingressQueue) {
      this.divertName = divertName;
      this.topicAddress = topicAddress;
      this.ingressAddress = ingressAddress;
      this.ingressQueue = ingressQueue;
    }
  }
}
