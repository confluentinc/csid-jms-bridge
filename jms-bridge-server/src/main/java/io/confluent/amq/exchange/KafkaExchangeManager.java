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
import io.confluent.amq.persistence.kafka.KafkaIO;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * <p>
 * Manages and maintains the active exchange of data between top level kafka topics and ActiveMQ
 * destinations.
 * </p>
 *
 * <p>
 * The manager will detect when either kafka or the bridge has changes that impacts the exchange.
 * When an impact does occur it will update the exchange to represent the new state.
 * </p>
 *
 * <p>
 * Potential changes that could impact the exchange are:
 * </p>
 * <ul>
 *   <li>Kafka security changes</li>
 *   <li>Kafka topic creation/deletion</li>
 *   <li>AMQ address binding changes</li>
 * </ul>
 */
@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaExchangeManager implements ActiveMQServerPlugin, ActivateCallback {

    private static final StructuredLogger SLOG = StructuredLogger
            .with(b -> b.loggerClass(KafkaExchangeManager.class));

    private final ComponentLifeCycle state = new ComponentLifeCycle(SLOG);
    private final AtomicReference<ConfluentAmqServer> amqServerRef = new AtomicReference<>();
    private final BridgeConfig bridgeConfig;
    private final RoutingConfig routingConfig;
    private final KafkaIO kafkaIO;

    private KafkaExchange exchange;
    private Map<KafkaTopicExchange, KafkaExchangeIngress> ingressMap = new ConcurrentHashMap<>();
    private Map<String, KafkaTopicExchange> ingressQueueMap = new ConcurrentHashMap<>();
    private KafkaExchangeEgress egress;
    private KafkaExchangeInterpreter exchangeInterpreter;

    public KafkaExchangeManager(
            BridgeConfig bridgeConfig,
            KafkaIO kafkaIO,
            KafkaExchangeInterpreter interpreter) {

        this.bridgeConfig = bridgeConfig;
        this.routingConfig = bridgeConfig.routing().orElse(null);
        this.kafkaIO = kafkaIO;
        this.exchangeInterpreter = interpreter;
    }

    public KafkaExchangeManager(
            BridgeConfig bridgeConfig,
            KafkaIO kafkaIO) {

        this(bridgeConfig, kafkaIO, new KafkaExchangeInterpreter(bridgeConfig));
    }

    public List<KafkaTopicExchange> getAllTopicExchanges() {
        return new ArrayList<>(ingressMap.keySet());
    }

    @Override
    public void registered(ActiveMQServer server) {
        SLOG.info(b -> b.event("Registered"));
        this.amqServerRef.set((ConfluentAmqServer) server);
        //TODO: See if there is a better place to init the KafkaIntegration.
        //needs to be runnable from ActiveMQServerImpl.start() as that is used by backup shared store activation on failover.
        try {
            getAmqServer().getKafkaIntegration().start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        state.doPrepare(this::prepare);
        server.registerActivateCallback(this);
    }

    @Override
    public void unregistered(ActiveMQServer server) {
        SLOG.info(b -> b.event("Unregistered"));
        this.amqServerRef.set(null);
    }

    @Override
    public void activated() {
        SLOG.info(b -> b.event("Activated"));
        start();
    }

    @Override
    public void deActivate() {
        SLOG.info(b -> b.event("DeActivated"));
        stop();
    }

    @VisibleForTesting
    protected void prepare(
            ConfluentAmqServer amqServer,
            KafkaExchange exchange,
            KafkaExchangeIngress ingress,
            KafkaExchangeEgress egress
    ) {
        state.doPrepare(() -> {
            this.amqServerRef.set(amqServer);
            this.exchange = exchange;
            this.egress = egress;
        });
    }

    protected void prepare() {
        this.exchange = new KafkaExchange();

        this.egress = new KafkaExchangeEgress(
                bridgeConfig,
                getAmqServer(),
                exchange,
                kafkaIO);
    }

    public boolean isEnabled() {
        return routingConfig != null;
    }

    public State getState() {
        return state.getState();
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

        getAmqServer().getScheduledPool().schedule(
                this::topicSyncTask, 1000, TimeUnit.MILLISECONDS);
    }

    private void doStart() {
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
            getAmqServer().getScheduledPool()
                    .schedule(this::topicSyncTask, 5000, TimeUnit.MILLISECONDS);
        } else {

            try {
                this.synchronizeTopics();
                SLOG.debug(b -> b
                        .event("ScheduleNextTopicSync")
                        .putTokens("delayMs", routingConfig.metadataRefreshMs())
                        .message("Scheduling next topic sync."));
            } catch (Exception e) {
                SLOG.error(b -> b
                        .event("StopTopicSyncTask")
                        .message("The topic sync task has failed."), e);
                throw new RuntimeException(e);
            }

            getAmqServer().getScheduledPool()
                    .schedule(this::topicSyncTask, routingConfig.metadataRefreshMs(), TimeUnit.MILLISECONDS);
        }
    }

    public void stop() {
        if (!isEnabled()) {
            return;
        }

        state.doStop(this::doStop);
    }

    private void doStop() {
        ingressMap.forEach((topicExchange, ingress) -> {
            try {
                ingress.stop();
            } catch (Exception e) {
                SLOG.error(b -> b
                        .event("Stop")
                        .markFailure()
                        .message("Failed to stop exchange ingress"), e);
            }
        });

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
        this.egress.updateConsumerTopics();
    }

    @Override
    public void afterRemoveBinding(Binding binding, Transaction tx, boolean deleteData)
            throws ActiveMQException {
        String jmsAddress = binding.getAddress().toString();

        this.exchange.removeReader(jmsAddress);
        this.egress.updateConsumerTopics();
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
        if (!getAmqServer().isActive()) {
            SLOG.info(b -> b.event("SynchronizeTopics")
                    .markFailure()
                    .message(
                            "Synchronizing of topics cannot be done when the server is not currently active."));
            return;
        }

        SLOG.info(b -> b.event("SynchronizeTopics"));
        //create the basic ingress parts: address, queue and binding
        Set<KafkaTopicExchange> discoveredExchanges = discoverTopicExchanges(routingConfig.topics());
        Set<KafkaTopicExchange> currentExchanges = exchange.getAllExchanges();
        Set<KafkaTopicExchange> newExchanges = Sets.difference(discoveredExchanges, currentExchanges);
        final Set<KafkaTopicExchange> removedExchanges = Sets
                .difference(currentExchanges, discoveredExchanges);

        Map<KafkaTopicExchange, KafkaExchangeInterpreter.IngressEntities> kteEntityMap = null;
        if (forceCreate) {
            kteEntityMap = createExchangeEntities(discoveredExchanges);
        } else {
            kteEntityMap = createExchangeEntities(newExchanges);
        }

        Map<KafkaTopicExchange, Integer> kteAccumMap = new HashMap<>();
        for (Entry<KafkaTopicExchange, KafkaExchangeInterpreter.IngressEntities> en
                : kteEntityMap.entrySet()) {
            int bindings = (int) getAmqServer().getPostOffice()
                    .getBindingsForAddress(SimpleString.toSimpleString(en.getKey().amqAddressName()))
                    .getBindings()
                    .stream()
                    .filter(b ->
                            Objects.equals(b.getAddress(), en.getValue().getIngressQueue().getAddress()))
                    .count();

            kteAccumMap.put(en.getKey(), bindings);
        }
        exchange.addTopicExchanges(kteAccumMap, true);

        for (KafkaTopicExchange discExchange : discoveredExchanges) {
            if (!ingressMap.containsKey(discExchange)) {
                Queue queue = getAmqServer().locateQueue(discExchange.ingressQueueName());
                KafkaExchangeIngress ingress = new KafkaExchangeIngress(
                        bridgeConfig,
                        discExchange,
                        kafkaIO,
                        queue,
                        getAmqServer().getStorageManager().generateID());

                ingressMap.put(discExchange, ingress);
                ingressQueueMap.put(discExchange.amqAddressName(), discExchange);
                ingress.start();
            }
        }

        for (KafkaTopicExchange kte : removedExchanges) {
            exchange.removeExchange(kte);
            if (ingressMap.containsKey(kte)) {
                try {
                    ingressQueueMap.remove(kte.amqAddressName());
                    ingressMap.remove(kte).stop();
                } catch (Exception e) {
                    SLOG.error(b -> b.event("StopRemovedIngressExchange")
                            .markFailure()
                            .putTokens("topicExchange", kte), e);
                }
            }
            deleteIngressEntities(kte);
        }

        egress.updateConsumerTopics();

        SLOG.info(b -> b.event("SynchronizeTopics").markCompleted());
    }

    /**
     * Attempts to cleanup AMQ entities associated to a kafka topic exchange (see {@link
     * KafkaExchangeInterpreter#createExchangeEntities(KafkaTopicExchange, ConfluentAmqServer)}).
     * This may not always be  possible due to existing
     * bindings and in that case they will be paused to prevent further publishing.
     *
     * @param kte the kafka topic exchange to pause or delete the entities for
     */
    protected void deleteIngressEntities(KafkaTopicExchange kte) throws Exception {
        exchangeInterpreter.deleteIngressEntities(kte, getAmqServer());
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
        Set<String> kafkaTopics = kafkaIO.listTopics();
        Set<KafkaTopicExchange> allExchanges = new HashSet<>();
        for (RoutedTopic routedTopic : topicRoutes) {
            SLOG.info(b -> b
                    .event("ProcessRoutingTopicRule")
                    .markStarted()
                    .putTokens("topicRule", routedTopic));

            Pattern matcher = Pattern.compile("^" + routedTopic.match() + "$");
            // TODO: exclude internal kcache topics here instead of streams wal topics
            Set<KafkaTopicExchange> exchanges = kafkaTopics.stream()
                    .filter(t -> !t
                            .contains(getAmqServer().getKafkaIntegration().getBindingsJournal().walTopic()))
                    .filter(t -> !t
                            .contains(getAmqServer().getKafkaIntegration().getMessagesJournal().walTopic()))
                    .filter(matcher.asPredicate())
                    .map(topic -> new Builder()
                            .originConfig(routedTopic)
                            .amqAddressName(routedTopic.addressTemplate().replace("${topic}", topic))
                            .kafkaTopicName(topic)
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
     * See {
     * {@link KafkaExchangeInterpreter#createExchangeEntities(KafkaTopicExchange, ConfluentAmqServer)}
     * .
     * This supports doing many at once.
     *
     * @return a map of entities created, one for each exchange
     */
    protected Map<KafkaTopicExchange, KafkaExchangeInterpreter.IngressEntities>
    createExchangeEntities(Set<KafkaTopicExchange> allExchanges) throws Exception {

        return allExchanges.stream()
                .collect(Collectors.toMap(
                        Function.identity(),
                        kte -> exchangeInterpreter.createExchangeEntities(kte, getAmqServer())));
    }

    @Override
    public void beforeMessageRoute(Message message, RoutingContext context, boolean direct,
                                   boolean rejectDuplicates) throws ActiveMQException {

        Optional<KafkaTopicExchange> kteOpt = Optional.empty();
        if (exchangeInterpreter.isRoutable(message.toCore())) {
            kteOpt = exchange.findByAddress(message.getAddress());
        }

        if (kteOpt.isPresent()) {
            message.putLongProperty(
                    Headers.EX_HDR_TS,
                    System.currentTimeMillis());
            message.putStringProperty(
                    Headers.EX_HDR_KAFKA_TOPIC,
                    kteOpt.get().kafkaTopicName());
            message.putBytesProperty(
                    Headers.EX_HDR_KAFKA_RECORD_KEY,
                    exchangeInterpreter.extractKey(kteOpt.get(), message));

            SLOG.trace(b -> b.name("BeforeMessageRoute")
                    .event("IsMessageRoutable")
                    .markSuccess()
                    .addAmqMessage(message));
        } else {
            SLOG.trace(b -> b.name("BeforeMessageRoute")
                    .event("IsMessageRoutable")
                    .markFailure()
                    .message("No KafkaExchangeTopic was mapped to destination address")
                    .addAmqMessage(message));
        }
    }

    private ConfluentAmqServer getAmqServer() {
        return amqServerRef.get();
    }

}
