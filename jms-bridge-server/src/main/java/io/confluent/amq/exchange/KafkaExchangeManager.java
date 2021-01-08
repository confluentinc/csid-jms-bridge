/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.confluent.amq.ComponentLifeCycle;
import io.confluent.amq.ComponentLifeCycle.State;
import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.exchange.KafkaTopicExchange.Builder;
import io.confluent.amq.logging.StructuredLogger;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
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
 *   <ul>
 *    <li>Kafka security changes</li>
 *    <li>Kafka topic creation/deletion</li>
 *    <li>AMQ address binding changes</li>
 *   </ul>
 * </p>
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaExchangeManager implements ActiveMQServerPlugin {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeManager.class));

  private final BridgeConfig bridgeConfig;
  private final RoutingConfig routingConfig;
  private final ConfluentAmqServer amqServer;
  private final KafkaExchange exchange;
  private final KafkaExchangeIngress ingress;
  private final KafkaExchangeEgress egress;

  private final ComponentLifeCycle state = new ComponentLifeCycle(SLOG);

  public KafkaExchangeManager(
      BridgeConfig bridgeConfig,
      ConfluentAmqServer amqServer) {

    this.bridgeConfig = bridgeConfig;
    this.routingConfig = bridgeConfig.routing().orElse(null);
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

  @VisibleForTesting
  protected KafkaExchangeManager(BridgeConfig bridgeConfig,
      ConfluentAmqServer amqServer, KafkaExchange exchange,
      KafkaExchangeIngress ingress, KafkaExchangeEgress egress) {

    this.bridgeConfig = bridgeConfig;
    this.routingConfig = bridgeConfig.routing().orElse(null);
    this.amqServer = amqServer;
    this.exchange = exchange;
    this.ingress = ingress;
    this.egress = egress;
  }

  public boolean isEnabled() {
    return routingConfig != null;
  }

  public State getState() {
    return state.getState();
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

  /**
   * Fetch the list of Kafka topics that are currently being consumed for propagation of it's
   * messages to the corresponding Kafka topic exchange address. An existing Kafka topic exchange
   * does not mean the corresponding Kafka topic is being consumed since the messages may not be
   * routable (unbound topic address).
   */
  public Collection<String> currentSubscribedKafkaTopics() {
    return this.egress.currentConsumerTopics();
  }

  public void start() {
    if (!isEnabled()) {
      return;
    }

    state.doStart(this::doStart);

    try {
      synchronizeTopics();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    amqServer.getScheduledPool().schedule(
        this::topicSyncTask, routingConfig.metadataRefreshMs(), TimeUnit.MILLISECONDS);
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
  }

  private void topicSyncTask() {
    if (!state.isStarted()) {
      SLOG.info(b -> b
          .event("StopTopicSyncTask")
          .message("The Kafka exchange manager is not running, stopping topic sync task."));
      return;
    }

    try {
      this.synchronizeTopics();
      SLOG.debug(b -> b
          .event("ScheduleNextTopicSync")
          .putTokens("delayMs", routingConfig.metadataRefreshMs())
          .message("Scheduling next topic sync."));
      amqServer.getScheduledPool()
          .schedule(this::topicSyncTask, routingConfig.metadataRefreshMs(), TimeUnit.MILLISECONDS);
    } catch (Exception e) {
      SLOG.error(b -> b
          .event("StopTopicSyncTask")
          .message("The topic sync task has failed."), e);
      throw new RuntimeException(e);
    }
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
    this.exchange.addReader(binding.getAddress().toString());
  }

  @Override
  public void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData)
      throws ActiveMQException {
    this.exchange.removeReader(binding.getAddress().toString());
  }

  public void prepare() {
    state.doPrepare(() -> {
      //nothing
    });
  }


  /**
   * Synchronize the current set of kafka topic exchanges against the current set of available Kafka
   * topics.  Adds, updates and removes exchanges as necessary based on the configured topic routing
   * rules. This must be called before changes in the set of available Kafka topics will be
   * reflected in AMQ.
   */
  public void synchronizeTopics() throws Exception {
    synchronizeTopics(false);
  }

  /**
   * Synchronize the current set of kafka topic exchanges against the current set of available Kafka
   * topics.  Adds, updates and removes exchanges as necessary based on the configured topic routing
   * rules.
   *
   * @param forceCreate force the recreation of all exchanges instead of only newly found ones.
   */
  protected void synchronizeTopics(boolean forceCreate) throws Exception {
    if (!state.isStarted()) {
      SLOG.info(b -> b.event("SynchronizeTopics")
          .markFailure()
          .putTokens("state", state.getState())
          .message("Synchronizing of topics cannot be done when not in a 'started' state."));
      return;
    }

    SLOG.info(b -> b.event("SynchronizeTopics"));
    //create the basic ingress parts: address, queue and binding
    Set<KafkaTopicExchange> discoveredExchanges = discoverTopicExchanges(routingConfig.topics());
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

    for (Entry<KafkaTopicExchange, IngressEntities> en : kteEntityMap.entrySet()) {
      int bindings = (int) amqServer.getPostOffice()
          .getBindingsForAddress(SimpleString.toSimpleString(en.getKey().amqAddressName()))
          .getBindings()
          .stream()
          .filter(b ->
              Objects.equals(Objects.toString(b.getUniqueName()), en.getValue().divertName))
          .count();

      exchange.addTopicExchange(en.getKey(), true, bindings);
    }

    for (KafkaTopicExchange kte : removedExchanges) {
      exchange.removeExchange(kte);
      deleteIngressEntities(kte);
    }

    SLOG.info(b -> b.event("SynchronizeTopics").markCompleted());
  }

  /**
   * Attempts to cleanup AMQ entities associated to a kafka topic exchange (see {@link
   * #createExchangeEntities(KafkaTopicExchange)}). This may not always be  possible due to existing
   * bindings and in that case they will be paused to prevent further publishing.
   *
   * @param kte the kafka topic exchange to pause or delete the entities for
   */
  protected void deleteIngressEntities(KafkaTopicExchange kte) throws Exception {
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

      amqServer.destroyDivert(SimpleString.toSimpleString(
          KafkaExchangeUtil.createDivertName(kte.amqAddressName())));
      amqServer.removeAddressInfo(SimpleString.toSimpleString(kte.ingressQueueName()), null, true);
      amqServer.removeAddressInfo(SimpleString.toSimpleString(kte.amqAddressName()), null, true);
    }
  }

  /**
   * <p>
   * Given a list of topic routes process each one against a all eligible Kafka topics. Eligible
   * topics are ones accessible by the JMS Bridge Kafka principal, are not internal journal topics
   * and do not begin with an underscore ('_').
   * </p>
   * <p>
   * The topic routes are processed in order with last in winning for each topic (in the case
   * multiple routes match a topic).
   * </p>
   *
   * @param topicRoutes the topic routing rules
   * @return set of uninitialized kafka topic exchanges
   */
  protected Set<KafkaTopicExchange> discoverTopicExchanges(List<RoutedTopic> topicRoutes) {
    //go through the topics and match them against the routed topics creating topic exchanges
    Set<String> kafkaTopics = amqServer.getKafkaIntegration().getKafkaIO().listTopics();
    Set<KafkaTopicExchange> allExchanges = new HashSet<>();
    for (RoutedTopic routedTopic : topicRoutes) {
      SLOG.info(b -> b
          .event("ProcessRoutingTopicRule")
          .markStarted()
          .putTokens("topicRule", routedTopic));

      Pattern matcher = Pattern.compile("^" + routedTopic.match() + "$");
      Set<KafkaTopicExchange> exchanges = kafkaTopics.stream()
          .filter(t -> !t.equals(amqServer.getKafkaIntegration().getBindingsJournal().walTopic()))
          .filter(t -> !t.equals(amqServer.getKafkaIntegration().getMessagesJournal().walTopic()))
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

  /**
   * See {@link #createExchangeEntities(KafkaTopicExchange)}. This supports doing many at once.
   *
   * @return a map of entities created, one for each exchange
   */
  protected Map<KafkaTopicExchange, IngressEntities> createExchangeEntities(
      Set<KafkaTopicExchange> allExchanges) throws Exception {

    return allExchanges.stream()
        .collect(Collectors.toMap(Function.identity(), this::createExchangeEntities));
  }

  /**
   * Creates and registeres the AMQ entities (Address, Queue, Divert) required for a topic exchange
   * to function fully. If the entities already exist then they will be updated if required and
   * returned.
   *
   * @return the AMQ entities created or found
   */
  protected IngressEntities createExchangeEntities(KafkaTopicExchange exchange) {

    try {
      final AddressInfo topicAddress = createIngressAddress(exchange.amqAddressName());
      final AddressInfo ingressAddress = createIngressAddress(exchange.ingressQueueName());

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

      return new IngressEntities(
          divertConfig.getName(), topicAddress, ingressAddress, ingressQueue);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  private AddressInfo createIngressAddress(String name) throws Exception {
    AddressInfo ingressAddress = new AddressInfo(
        SimpleString.toSimpleString(name),
        RoutingType.MULTICAST);

    return amqServer.addOrUpdateAddressInfo(ingressAddress);
  }

  private Queue createIngressQueue(String addressName, String name) throws Exception {
    QueueConfiguration ingressQueueConf = new QueueConfiguration(name)
        .setAddress(addressName)
        .setRoutingType(RoutingType.MULTICAST)
        .setDurable(true);
    Queue queue = amqServer.locateQueue(name);
    if (queue == null) {
      queue = amqServer.createQueue(ingressQueueConf);
    } else {
      queue = amqServer.updateQueue(ingressQueueConf);
    }
    return queue;
  }

  /**
   * A JMS/AMQ filter that that is applied to the divert that forwards messages to Kafka. Breaks
   * potential data cycles by filtering out data that originated from Kafka.
   */
  protected String divertFilter() {
    String hopsHeaderKey = Headers.createHopsKey(bridgeConfig.id());
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
