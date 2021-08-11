/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.RoutingType;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.management.impl.ActiveMQServerControlImpl;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.OperationContext;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.remoting.server.RemotingService;
import org.apache.activemq.artemis.core.replication.ReplicationEndpoint;
import org.apache.activemq.artemis.core.replication.ReplicationManager;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.security.SecurityAuth;
import org.apache.activemq.artemis.core.security.SecurityStore;
import org.apache.activemq.artemis.core.server.ActivateCallback;
import org.apache.activemq.artemis.core.server.ActivationFailureListener;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.AddressQueryResult;
import org.apache.activemq.artemis.core.server.BindingQueryResult;
import org.apache.activemq.artemis.core.server.NetworkHealthCheck;
import org.apache.activemq.artemis.core.server.NodeManager;
import org.apache.activemq.artemis.core.server.PostQueueCreationCallback;
import org.apache.activemq.artemis.core.server.PostQueueDeletionCallback;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.QueueFactory;
import org.apache.activemq.artemis.core.server.QueueQueryResult;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.cluster.BackupManager;
import org.apache.activemq.artemis.core.server.cluster.ClusterManager;
import org.apache.activemq.artemis.core.server.cluster.ha.HAPolicy;
import org.apache.activemq.artemis.core.server.federation.FederationManager;
import org.apache.activemq.artemis.core.server.files.FileStoreMonitor;
import org.apache.activemq.artemis.core.server.group.GroupingHandler;
import org.apache.activemq.artemis.core.server.impl.Activation;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.impl.ConnectorsService;
import org.apache.activemq.artemis.core.server.impl.SharedNothingLiveActivation;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.apache.activemq.artemis.core.server.metrics.MetricsManager;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQPluginRunnable;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerAddressPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBasePlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBindingPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerBridgePlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConnectionPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerConsumerPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerCriticalPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerFederationPlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerMessagePlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerQueuePlugin;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerSessionPlugin;
import org.apache.activemq.artemis.core.server.reload.ReloadManager;
import org.apache.activemq.artemis.core.settings.HierarchicalRepository;
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.core.version.Version;
import org.apache.activemq.artemis.spi.core.protocol.ProtocolManagerFactory;
import org.apache.activemq.artemis.spi.core.protocol.RemotingConnection;
import org.apache.activemq.artemis.spi.core.protocol.SessionCallback;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;

@SuppressWarnings({"checkstyle:ParameterNumber", "unused", "rawtypes"})
public class DelegatingConfluentAmqServer implements ActiveMQServer {

  private final ConfluentAmqServerImpl confluentAmqServer;

  public DelegatingConfluentAmqServer() {
    confluentAmqServer = new ConfluentAmqServerImpl();
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration);
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQServer parentServer) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration, parentServer);
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration, mbeanServer);
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final ActiveMQSecurityManager securityManager) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration, securityManager);
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration, mbeanServer, securityManager);
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration, mbeanServer, securityManager,
        parentServer);
  }

  public DelegatingConfluentAmqServer(final JmsBridgeConfiguration configuration,
      final MBeanServer mbeanServer,
      final ActiveMQSecurityManager securityManager, final ActiveMQServer parentServer,
      final ServiceRegistry serviceRegistry) {
    confluentAmqServer = new ConfluentAmqServerImpl(configuration, mbeanServer, securityManager,
        parentServer, serviceRegistry);
  }

  public ConfluentAmqServer unwrap() {
    return confluentAmqServer;
  }

  /**
   * Delegates to a method that is not final and can be overridden by the ConfluentAmqServer
   */
  @Override
  public void fail(boolean failoverOnServerShutdown) throws Exception {
    confluentAmqServer.doFail(failoverOnServerShutdown);
  }

  /**
   * Delegates to a method that is not final and can be overridden by the ConfluentAmqServer
   */
  @Override
  public void start() throws Exception {
    confluentAmqServer.doStart();
  }

  /**
   * Delegates to a method that is not final and can be overridden by the ConfluentAmqServer
   */
  public void stopTheServer(boolean criticalIOError) {
    confluentAmqServer.doStopTheServer(criticalIOError);
  }

  /**
   * Delegates to a method that is not final and can be overridden by the ConfluentAmqServer
   */
  @Override
  public void stop(boolean failoverOnServerShutdown, boolean isExit) throws Exception {
    confluentAmqServer.doStop(failoverOnServerShutdown, isExit);
  }

  @Override
  public void stop() throws Exception {
    confluentAmqServer.stop();
  }

  @Override
  public void stop(boolean isShutdown) throws Exception {
    confluentAmqServer.stop(isShutdown);
  }

  @SuppressWarnings("checkstyle:AbbreviationAsWordInName")
  @Override
  public BindingQueryResult bindingQuery(
      SimpleString address, boolean newFQQN) throws Exception {
    return confluentAmqServer.bindingQuery(address, newFQQN);
  }

  @Override
  public BindingQueryResult bindingQuery(
      SimpleString address) throws Exception {
    return confluentAmqServer.bindingQuery(address);
  }

  public Runnable getAfterActivationCreated() {
    return confluentAmqServer.getAfterActivationCreated();
  }

  public ActiveMQServerImpl setAfterActivationCreated(Runnable afterActivationCreated) {
    return confluentAmqServer.setAfterActivationCreated(afterActivationCreated);
  }

  @Override
  public OperationContext newOperationContext() {
    return confluentAmqServer.newOperationContext();
  }

  @Override
  public CriticalAnalyzer getCriticalAnalyzer() {
    return confluentAmqServer.getCriticalAnalyzer();
  }

  @Override
  public ReplicationEndpoint getReplicationEndpoint() {
    return confluentAmqServer.getReplicationEndpoint();
  }

  @Override
  public void unlockActivation() {
    confluentAmqServer.unlockActivation();
  }

  @Override
  public void lockActivation() {
    confluentAmqServer.lockActivation();
  }

  public void interruptActivationThread(
      NodeManager nodeManagerInUse) throws InterruptedException {
    confluentAmqServer.interruptActivationThread(nodeManagerInUse);
  }

  @Override
  public Activation getActivation() {
    return confluentAmqServer.getActivation();
  }

  @SuppressWarnings("rawtypes")
  @Override
  public HAPolicy getHAPolicy() {
    return confluentAmqServer.getHAPolicy();
  }

  @Override
  public void setHAPolicy(HAPolicy haPolicy) {
    confluentAmqServer.setHAPolicy(haPolicy);
  }

  @Override
  public void setMBeanServer(MBeanServer mbeanServer) {
    confluentAmqServer.setMBeanServer(mbeanServer);
  }

  @Override
  public void addExternalComponent(
      ActiveMQComponent externalComponent) {
    confluentAmqServer.addExternalComponent(externalComponent);
  }

  @Override
  public ExecutorService getThreadPool() {
    return confluentAmqServer.getThreadPool();
  }

  public void setActivation(
      SharedNothingLiveActivation activation) {
    confluentAmqServer.setActivation(activation);
  }


  @Override
  public void addActivationParam(String key, Object val) {
    confluentAmqServer.addActivationParam(key, val);
  }

  @Override
  public boolean isAddressBound(String address) throws Exception {
    return confluentAmqServer.isAddressBound(address);
  }


  @Override
  public QueueQueryResult queueQuery(
      SimpleString name) {
    return confluentAmqServer.queueQuery(name);
  }

  @Override
  public AddressQueryResult addressQuery(
      SimpleString name) throws Exception {
    return confluentAmqServer.addressQuery(name);
  }

  @Override
  public void threadDump() {
    confluentAmqServer.threadDump();
  }

  @Override
  public boolean isReplicaSync() {
    return confluentAmqServer.isReplicaSync();
  }

  public boolean checkLiveIsNotColocated(String nodeId) {
    return confluentAmqServer.checkLiveIsNotColocated(nodeId);
  }

  @Override
  public String describe() {
    return confluentAmqServer.describe();
  }

  @Override
  public String destroyConnectionWithSessionMetadata(String metaKey, String parameterValue)
      throws Exception {
    return confluentAmqServer.destroyConnectionWithSessionMetadata(metaKey, parameterValue);
  }

  @Override
  public void setIdentity(String identity) {
    confluentAmqServer.setIdentity(identity);
  }

  @Override
  public String getIdentity() {
    return confluentAmqServer.getIdentity();
  }

  @Override
  public ScheduledExecutorService getScheduledPool() {
    return confluentAmqServer.getScheduledPool();
  }

  @Override
  public Configuration getConfiguration() {
    return confluentAmqServer.getConfiguration();
  }

  @Override
  public PagingManager getPagingManager() {
    return confluentAmqServer.getPagingManager();
  }

  @Override
  public RemotingService getRemotingService() {
    return confluentAmqServer.getRemotingService();
  }

  @Override
  public StorageManager getStorageManager() {
    return confluentAmqServer.getStorageManager();
  }

  @Override
  public ActiveMQSecurityManager getSecurityManager() {
    return confluentAmqServer.getSecurityManager();
  }

  @Override
  public ManagementService getManagementService() {
    return confluentAmqServer.getManagementService();
  }

  @Override
  public HierarchicalRepository<Set<Role>> getSecurityRepository() {
    return confluentAmqServer.getSecurityRepository();
  }

  @Override
  public NodeManager getNodeManager() {
    return confluentAmqServer.getNodeManager();
  }

  @Override
  public HierarchicalRepository<AddressSettings> getAddressSettingsRepository() {
    return confluentAmqServer.getAddressSettingsRepository();
  }

  @Override
  public ResourceManager getResourceManager() {
    return confluentAmqServer.getResourceManager();
  }

  @Override
  public MetricsManager getMetricsManager() {
    return confluentAmqServer.getMetricsManager();
  }

  @Override
  public Version getVersion() {
    return confluentAmqServer.getVersion();
  }

  @Override
  public boolean isStarted() {
    return confluentAmqServer.isStarted();
  }

  @Override
  public ClusterManager getClusterManager() {
    return confluentAmqServer.getClusterManager();
  }

  public BackupManager getBackupManager() {
    return confluentAmqServer.getBackupManager();
  }

  @Override
  public ServerSession createSession(String name,
      String username, String password, int minLargeMessageSize,
      RemotingConnection connection,
      boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge, boolean xa,
      String defaultAddress, SessionCallback callback,
      boolean autoCreateQueues, OperationContext context,
      Map<SimpleString, RoutingType> prefixes,
      String securityDomain) throws Exception {
    return confluentAmqServer
        .createSession(name, username, password, minLargeMessageSize, connection, autoCommitSends,
            autoCommitAcks, preAcknowledge, xa, defaultAddress, callback, autoCreateQueues, context,
            prefixes, securityDomain);
  }

  @Override
  public void checkQueueCreationLimit(String username) throws Exception {
    confluentAmqServer.checkQueueCreationLimit(username);
  }

  public int getQueueCountForUser(String username) throws Exception {
    return confluentAmqServer.getQueueCountForUser(username);
  }

  @Override
  public SecurityStore getSecurityStore() {
    return confluentAmqServer.getSecurityStore();
  }

  @Override
  public void removeSession(String name) throws Exception {
    confluentAmqServer.removeSession(name);
  }

  @Override
  public ServerSession lookupSession(String key,
      String value) {
    return confluentAmqServer.lookupSession(key, value);
  }

  @Override
  public List<ServerSession> getSessions(
      String connectionID) {
    return confluentAmqServer.getSessions(connectionID);
  }

  @Override
  public Set<ServerSession> getSessions() {
    return confluentAmqServer.getSessions();
  }

  @Override
  public boolean isActive() {
    return confluentAmqServer.isActive();
  }

  @Override
  public boolean waitForActivation(long timeout, TimeUnit unit) throws InterruptedException {
    return confluentAmqServer.waitForActivation(timeout, unit);
  }

  @Override
  public ActiveMQServerControlImpl getActiveMQServerControl() {
    return confluentAmqServer.getActiveMQServerControl();
  }

  @Override
  public int getConnectionCount() {
    return confluentAmqServer.getConnectionCount();
  }

  @Override
  public long getTotalConnectionCount() {
    return confluentAmqServer.getTotalConnectionCount();
  }

  @Override
  public long getTotalMessageCount() {
    return confluentAmqServer.getTotalMessageCount();
  }

  @Override
  public long getTotalMessagesAdded() {
    return confluentAmqServer.getTotalMessagesAdded();
  }

  @Override
  public long getTotalMessagesAcknowledged() {
    return confluentAmqServer.getTotalMessagesAcknowledged();
  }

  @Override
  public long getTotalConsumerCount() {
    return confluentAmqServer.getTotalConsumerCount();
  }

  @Override
  public PostOffice getPostOffice() {
    return confluentAmqServer.getPostOffice();
  }

  @Override
  public QueueFactory getQueueFactory() {
    return confluentAmqServer.getQueueFactory();
  }

  @Override
  public SimpleString getNodeID() {
    return confluentAmqServer.getNodeID();
  }


  @Override
  @Deprecated
  public void createSharedQueue(SimpleString address,
      RoutingType routingType,
      SimpleString name, SimpleString filterString,
      SimpleString user, boolean durable) throws Exception {
    confluentAmqServer.createSharedQueue(address, routingType, name, filterString, user, durable);
  }

  @Override
  @Deprecated
  public void createSharedQueue(SimpleString address,
      RoutingType routingType,
      SimpleString name, SimpleString filterString,
      SimpleString user, boolean durable, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean lastValue) throws Exception {
    confluentAmqServer
        .createSharedQueue(address, routingType, name, filterString, user, durable, maxConsumers,
            purgeOnNoConsumers, exclusive, lastValue);
  }

  @Override
  @Deprecated
  public void createSharedQueue(SimpleString address,
      RoutingType routingType,
      SimpleString name, SimpleString filterString,
      SimpleString user, boolean durable, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean groupRebalance, int groupBuckets, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount) throws Exception {
    confluentAmqServer
        .createSharedQueue(address, routingType, name, filterString, user, durable, maxConsumers,
            purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets, lastValue, lastValueKey,
            nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
            autoDeleteDelay,
            autoDeleteMessageCount);
  }

  @Override
  @Deprecated
  public void createSharedQueue(SimpleString address,
      RoutingType routingType,
      SimpleString name, SimpleString filterString,
      SimpleString user, boolean durable, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean groupRebalance, int groupBuckets,
      SimpleString groupFirstKey, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount) throws Exception {
    confluentAmqServer
        .createSharedQueue(address, routingType, name, filterString, user, durable, maxConsumers,
            purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets, groupFirstKey, lastValue,
            lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
            autoDeleteDelay, autoDeleteMessageCount);
  }

  @Override
  public void createSharedQueue(
      QueueConfiguration queueConfiguration) throws Exception {
    confluentAmqServer.createSharedQueue(queueConfiguration);
  }

  @Override
  public Queue locateQueue(SimpleString queueName) {
    return confluentAmqServer.locateQueue(queueName);
  }

  @Override
  public Queue locateQueue(String queueName) {
    return confluentAmqServer.locateQueue(queueName);
  }

  @Override
  @Deprecated
  public Queue deployQueue(SimpleString address,
      SimpleString resourceName, SimpleString filterString, boolean durable, boolean temporary)
      throws Exception {
    return confluentAmqServer.deployQueue(address, resourceName, filterString, durable, temporary);
  }

  @Override
  @Deprecated
  public Queue deployQueue(String address, String resourceName, String filterString,
      boolean durable,
      boolean temporary) throws Exception {
    return confluentAmqServer.deployQueue(address, resourceName, filterString, durable, temporary);
  }

  @Override
  public void destroyQueue(SimpleString queueName) throws Exception {
    confluentAmqServer.destroyQueue(queueName);
  }

  @Override
  public void destroyQueue(SimpleString queueName,
      SecurityAuth session) throws Exception {
    confluentAmqServer.destroyQueue(queueName, session);
  }

  @Override
  public void destroyQueue(SimpleString queueName,
      SecurityAuth session, boolean checkConsumerCount) throws Exception {
    confluentAmqServer.destroyQueue(queueName, session, checkConsumerCount);
  }

  @Override
  public void destroyQueue(SimpleString queueName,
      SecurityAuth session, boolean checkConsumerCount,
      boolean removeConsumers) throws Exception {
    confluentAmqServer.destroyQueue(queueName, session, checkConsumerCount, removeConsumers);
  }

  @Override
  public void destroyQueue(SimpleString queueName,
      SecurityAuth session, boolean checkConsumerCount,
      boolean removeConsumers, boolean autoDeleteAddress) throws Exception {
    confluentAmqServer
        .destroyQueue(queueName, session, checkConsumerCount, removeConsumers, autoDeleteAddress);
  }

  @Override
  public void destroyQueue(SimpleString queueName,
      SecurityAuth session, boolean checkConsumerCount,
      boolean removeConsumers, boolean autoDeleteAddress, boolean checkMessageCount)
      throws Exception {
    confluentAmqServer
        .destroyQueue(queueName, session, checkConsumerCount, removeConsumers, autoDeleteAddress,
            checkMessageCount);
  }

  @Override
  public void clearAddressCache() {
    confluentAmqServer.clearAddressCache();
  }

  @Override
  public void registerActivateCallback(
      ActivateCallback callback) {
    confluentAmqServer.registerActivateCallback(callback);
  }

  @Override
  public void unregisterActivateCallback(
      ActivateCallback callback) {
    confluentAmqServer.unregisterActivateCallback(callback);
  }

  @Override
  public void registerActivationFailureListener(
      ActivationFailureListener listener) {
    confluentAmqServer.registerActivationFailureListener(listener);
  }

  @Override
  public void unregisterActivationFailureListener(
      ActivationFailureListener listener) {
    confluentAmqServer.unregisterActivationFailureListener(listener);
  }

  @Override
  public void callActivationFailureListeners(Exception e) {
    confluentAmqServer.callActivationFailureListeners(e);
  }

  @Override
  public void registerPostQueueCreationCallback(
      PostQueueCreationCallback callback) {
    confluentAmqServer.registerPostQueueCreationCallback(callback);
  }

  @Override
  public void unregisterPostQueueCreationCallback(
      PostQueueCreationCallback callback) {
    confluentAmqServer.unregisterPostQueueCreationCallback(callback);
  }

  @Override
  public void callPostQueueCreationCallbacks(
      SimpleString queueName) throws Exception {
    confluentAmqServer.callPostQueueCreationCallbacks(queueName);
  }

  @Override
  public void registerPostQueueDeletionCallback(
      PostQueueDeletionCallback callback) {
    confluentAmqServer.registerPostQueueDeletionCallback(callback);
  }

  @Override
  public void unregisterPostQueueDeletionCallback(
      PostQueueDeletionCallback callback) {
    confluentAmqServer.unregisterPostQueueDeletionCallback(callback);
  }

  @Override
  public void callPostQueueDeletionCallbacks(
      SimpleString address, SimpleString queueName) throws Exception {
    confluentAmqServer.callPostQueueDeletionCallbacks(address, queueName);
  }

  @Override
  public void registerBrokerPlugins(
      List<ActiveMQServerBasePlugin> plugins) {
    confluentAmqServer.registerBrokerPlugins(plugins);
  }

  @Override
  public void registerBrokerPlugin(
      ActiveMQServerBasePlugin plugin) {
    confluentAmqServer.registerBrokerPlugin(plugin);
  }

  @Override
  public void unRegisterBrokerPlugin(
      ActiveMQServerBasePlugin plugin) {
    confluentAmqServer.unRegisterBrokerPlugin(plugin);
  }

  @Override
  public List<ActiveMQServerBasePlugin> getBrokerPlugins() {
    return confluentAmqServer.getBrokerPlugins();
  }

  @Override
  public List<ActiveMQServerConnectionPlugin> getBrokerConnectionPlugins() {
    return confluentAmqServer.getBrokerConnectionPlugins();
  }

  @Override
  public List<ActiveMQServerSessionPlugin> getBrokerSessionPlugins() {
    return confluentAmqServer.getBrokerSessionPlugins();
  }

  @Override
  public List<ActiveMQServerConsumerPlugin> getBrokerConsumerPlugins() {
    return confluentAmqServer.getBrokerConsumerPlugins();
  }

  @Override
  public List<ActiveMQServerAddressPlugin> getBrokerAddressPlugins() {
    return confluentAmqServer.getBrokerAddressPlugins();
  }

  @Override
  public List<ActiveMQServerQueuePlugin> getBrokerQueuePlugins() {
    return confluentAmqServer.getBrokerQueuePlugins();
  }

  @Override
  public List<ActiveMQServerBindingPlugin> getBrokerBindingPlugins() {
    return confluentAmqServer.getBrokerBindingPlugins();
  }

  @Override
  public List<ActiveMQServerMessagePlugin> getBrokerMessagePlugins() {
    return confluentAmqServer.getBrokerMessagePlugins();
  }

  @Override
  public List<ActiveMQServerBridgePlugin> getBrokerBridgePlugins() {
    return confluentAmqServer.getBrokerBridgePlugins();
  }

  @Override
  public List<ActiveMQServerCriticalPlugin> getBrokerCriticalPlugins() {
    return confluentAmqServer.getBrokerCriticalPlugins();
  }

  @Override
  public List<ActiveMQServerFederationPlugin> getBrokerFederationPlugins() {
    return confluentAmqServer.getBrokerFederationPlugins();
  }

  @Override
  public void callBrokerPlugins(
      ActiveMQPluginRunnable pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerPlugins(pluginRun);
  }

  @Override
  public void callBrokerConnectionPlugins(
      ActiveMQPluginRunnable<ActiveMQServerConnectionPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerConnectionPlugins(pluginRun);
  }

  @Override
  public void callBrokerSessionPlugins(
      ActiveMQPluginRunnable<ActiveMQServerSessionPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerSessionPlugins(pluginRun);
  }

  @Override
  public void callBrokerConsumerPlugins(
      ActiveMQPluginRunnable<ActiveMQServerConsumerPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerConsumerPlugins(pluginRun);
  }

  @Override
  public void callBrokerAddressPlugins(
      ActiveMQPluginRunnable<ActiveMQServerAddressPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerAddressPlugins(pluginRun);
  }

  @Override
  public void callBrokerQueuePlugins(
      ActiveMQPluginRunnable<ActiveMQServerQueuePlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerQueuePlugins(pluginRun);
  }

  @Override
  public void callBrokerBindingPlugins(
      ActiveMQPluginRunnable<ActiveMQServerBindingPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerBindingPlugins(pluginRun);
  }

  @Override
  public void callBrokerMessagePlugins(
      ActiveMQPluginRunnable<ActiveMQServerMessagePlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerMessagePlugins(pluginRun);
  }

  @Override
  public void callBrokerBridgePlugins(
      ActiveMQPluginRunnable<ActiveMQServerBridgePlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerBridgePlugins(pluginRun);
  }

  @Override
  public void callBrokerCriticalPlugins(
      ActiveMQPluginRunnable<ActiveMQServerCriticalPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerCriticalPlugins(pluginRun);
  }

  @Override
  public void callBrokerFederationPlugins(
      ActiveMQPluginRunnable<ActiveMQServerFederationPlugin> pluginRun) throws ActiveMQException {
    confluentAmqServer.callBrokerFederationPlugins(pluginRun);
  }

  @Override
  public boolean hasBrokerPlugins() {
    return confluentAmqServer.hasBrokerPlugins();
  }

  @Override
  public boolean hasBrokerConnectionPlugins() {
    return confluentAmqServer.hasBrokerConnectionPlugins();
  }

  @Override
  public boolean hasBrokerSessionPlugins() {
    return confluentAmqServer.hasBrokerSessionPlugins();
  }

  @Override
  public boolean hasBrokerConsumerPlugins() {
    return confluentAmqServer.hasBrokerConsumerPlugins();
  }

  @Override
  public boolean hasBrokerAddressPlugins() {
    return confluentAmqServer.hasBrokerAddressPlugins();
  }

  @Override
  public boolean hasBrokerQueuePlugins() {
    return confluentAmqServer.hasBrokerQueuePlugins();
  }

  @Override
  public boolean hasBrokerBindingPlugins() {
    return confluentAmqServer.hasBrokerBindingPlugins();
  }

  @Override
  public boolean hasBrokerMessagePlugins() {
    return confluentAmqServer.hasBrokerMessagePlugins();
  }

  @Override
  public boolean hasBrokerBridgePlugins() {
    return confluentAmqServer.hasBrokerBridgePlugins();
  }

  @Override
  public boolean hasBrokerCriticalPlugins() {
    return confluentAmqServer.hasBrokerCriticalPlugins();
  }

  @Override
  public boolean hasBrokerFederationPlugins() {
    return confluentAmqServer.hasBrokerFederationPlugins();
  }

  @Override
  public ExecutorFactory getExecutorFactory() {
    return confluentAmqServer.getExecutorFactory();
  }

  @Override
  public ExecutorFactory getIOExecutorFactory() {
    return confluentAmqServer.getIOExecutorFactory();
  }

  @Override
  public void setGroupingHandler(
      GroupingHandler groupingHandler) {
    confluentAmqServer.setGroupingHandler(groupingHandler);
  }

  @Override
  public GroupingHandler getGroupingHandler() {
    return confluentAmqServer.getGroupingHandler();
  }

  @Override
  public ReplicationManager getReplicationManager() {
    return confluentAmqServer.getReplicationManager();
  }

  @Override
  public ConnectorsService getConnectorsService() {
    return confluentAmqServer.getConnectorsService();
  }

  @Override
  public FederationManager getFederationManager() {
    return confluentAmqServer.getFederationManager();
  }

  @Override
  public void deployDivert(DivertConfiguration config) throws Exception {
    confluentAmqServer.deployDivert(config);
  }

  @Override
  public void destroyDivert(SimpleString name) throws Exception {
    confluentAmqServer.destroyDivert(name);
  }

  @Override
  public void deployBridge(BridgeConfiguration config) throws Exception {
    confluentAmqServer.deployBridge(config);
  }

  @Override
  public void destroyBridge(String name) throws Exception {
    confluentAmqServer.destroyBridge(name);
  }

  @Override
  public void deployFederation(FederationConfiguration config) throws Exception {
    confluentAmqServer.deployFederation(config);
  }

  @Override
  public void undeployFederation(String name) throws Exception {
    confluentAmqServer.undeployFederation(name);
  }

  @Override
  public ServerSession getSessionByID(
      String sessionName) {
    return confluentAmqServer.getSessionByID(sessionName);
  }

  @Override
  public String toString() {
    return confluentAmqServer.toString();
  }

  public void replaceQueueFactory(QueueFactory factory) {
    confluentAmqServer.replaceQueueFactory(factory);
  }

  @Override
  public PagingManager createPagingManager() throws Exception {
    return confluentAmqServer.createPagingManager();
  }

  @Override
  public ServiceRegistry getServiceRegistry() {
    return confluentAmqServer.getServiceRegistry();
  }

  public void injectMonitor(
      FileStoreMonitor storeMonitor) throws Exception {
    confluentAmqServer.injectMonitor(storeMonitor);
  }

  public FileStoreMonitor getMonitor() {
    return confluentAmqServer.getMonitor();
  }

  public void completeActivation() throws Exception {
    confluentAmqServer.completeActivation();
  }

  @Override
  public boolean updateAddressInfo(SimpleString address,
      EnumSet<RoutingType> routingTypes) throws Exception {
    return confluentAmqServer.updateAddressInfo(address, routingTypes);
  }

  @Override
  @Deprecated
  public boolean updateAddressInfo(SimpleString address,
      Collection<RoutingType> routingTypes) throws Exception {
    return confluentAmqServer.updateAddressInfo(address, routingTypes);
  }

  @Override
  public boolean addAddressInfo(AddressInfo addressInfo) throws Exception {
    return confluentAmqServer.addAddressInfo(addressInfo);
  }

  @Override
  public AddressInfo addOrUpdateAddressInfo(
      AddressInfo addressInfo) throws Exception {
    return confluentAmqServer.addOrUpdateAddressInfo(addressInfo);
  }

  @Override
  public void removeAddressInfo(SimpleString address,
      SecurityAuth auth) throws Exception {
    confluentAmqServer.removeAddressInfo(address, auth);
  }

  @Override
  public void removeAddressInfo(SimpleString address,
      SecurityAuth auth, boolean force) throws Exception {
    confluentAmqServer.removeAddressInfo(address, auth, force);
  }

  @Override
  public String getInternalNamingPrefix() {
    return confluentAmqServer.getInternalNamingPrefix();
  }

  @Override
  public AddressInfo getAddressInfo(SimpleString address) {
    return confluentAmqServer.getAddressInfo(address);
  }

  @Override
  public void setState(SERVER_STATE state) {
    confluentAmqServer.setState(state);
  }

  @Override
  public SERVER_STATE getState() {
    return confluentAmqServer.getState();
  }

  @Override
  public ReloadManager getReloadManager() {
    return confluentAmqServer.getReloadManager();
  }

  @Override
  public NetworkHealthCheck getNetworkHealthCheck() {
    return confluentAmqServer.getNetworkHealthCheck();
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filterString, boolean durable, boolean temporary)
      throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filterString, durable, temporary);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString user,
      SimpleString filterString, boolean durable, boolean temporary) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, user, filterString, durable, temporary);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter, boolean durable, boolean temporary,
      int maxConsumers, boolean purgeOnNoConsumers, boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, durable, temporary, maxConsumers,
            purgeOnNoConsumers, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter, boolean durable, boolean temporary,
      int maxConsumers, boolean purgeOnNoConsumers, boolean exclusive, boolean groupRebalance,
      int groupBuckets, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount,
      boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, durable, temporary, maxConsumers,
            purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets, lastValue, lastValueKey,
            nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
            autoDeleteDelay,
            autoDeleteMessageCount, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter, boolean durable, boolean temporary,
      int maxConsumers, boolean purgeOnNoConsumers, boolean exclusive, boolean groupRebalance,
      int groupBuckets, SimpleString groupFirstKey, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount,
      boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, durable, temporary, maxConsumers,
            purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets, groupFirstKey, lastValue,
            lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
            autoDeleteDelay, autoDeleteMessageCount, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter, boolean durable, boolean temporary,
      int maxConsumers, boolean purgeOnNoConsumers, boolean exclusive, boolean groupRebalance,
      int groupBuckets, SimpleString groupFirstKey, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount,
      boolean autoCreateAddress, long ringSize) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, durable, temporary, maxConsumers,
            purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets, groupFirstKey, lastValue,
            lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
            autoDeleteDelay, autoDeleteMessageCount, autoCreateAddress, ringSize);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean autoCreated,
      Integer maxConsumers, Boolean purgeOnNoConsumers, boolean autoCreateAddress)
      throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, user, durable, temporary, autoCreated,
            maxConsumers, purgeOnNoConsumers, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(AddressInfo addressInfo,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean autoCreated,
      Integer maxConsumers, Boolean purgeOnNoConsumers, boolean autoCreateAddress)
      throws Exception {
    return confluentAmqServer
        .createQueue(addressInfo, queueName, filter, user, durable, temporary, autoCreated,
            maxConsumers, purgeOnNoConsumers, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(AddressInfo addressInfo,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean autoCreated,
      Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive,
      Boolean lastValue, boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(addressInfo, queueName, filter, user, durable, temporary, autoCreated,
            maxConsumers, purgeOnNoConsumers, exclusive, lastValue, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(AddressInfo addressInfo,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean autoCreated,
      Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive,
      Boolean groupRebalance, Integer groupBuckets, Boolean lastValue,
      SimpleString lastValueKey, Boolean nonDestructive, Integer consumersBeforeDispatch,
      Long delayBeforeDispatch, Boolean autoDelete, Long autoDeleteDelay,
      Long autoDeleteMessageCount, boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(addressInfo, queueName, filter, user, durable, temporary, autoCreated,
            maxConsumers, purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets, lastValue,
            lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch, autoDelete,
            autoDeleteDelay, autoDeleteMessageCount, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(AddressInfo addressInfo,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean autoCreated,
      Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive,
      Boolean groupRebalance, Integer groupBuckets,
      SimpleString groupFirstKey, Boolean lastValue,
      SimpleString lastValueKey, Boolean nonDestructive, Integer consumersBeforeDispatch,
      Long delayBeforeDispatch, Boolean autoDelete, Long autoDeleteDelay,
      Long autoDeleteMessageCount, boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(addressInfo, queueName, filter, user, durable, temporary, autoCreated,
            maxConsumers, purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets,
            groupFirstKey,
            lastValue, lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch,
            autoDelete, autoDeleteDelay, autoDeleteMessageCount, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(AddressInfo addressInfo,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean autoCreated,
      Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive,
      Boolean groupRebalance, Integer groupBuckets,
      SimpleString groupFirstKey, Boolean lastValue,
      SimpleString lastValueKey, Boolean nonDestructive, Integer consumersBeforeDispatch,
      Long delayBeforeDispatch, Boolean autoDelete, Long autoDeleteDelay,
      Long autoDeleteMessageCount, boolean autoCreateAddress, Long ringSize) throws Exception {
    return confluentAmqServer
        .createQueue(addressInfo, queueName, filter, user, durable, temporary, autoCreated,
            maxConsumers, purgeOnNoConsumers, exclusive, groupRebalance, groupBuckets,
            groupFirstKey,
            lastValue, lastValueKey, nonDestructive, consumersBeforeDispatch, delayBeforeDispatch,
            autoDelete, autoDeleteDelay, autoDeleteMessageCount, autoCreateAddress, ringSize);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean ignoreIfExists,
      boolean transientQueue, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers,
      boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, user, durable, temporary,
            ignoreIfExists,
            transientQueue, autoCreated, maxConsumers, purgeOnNoConsumers, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filter,
      SimpleString user, boolean durable, boolean temporary, boolean ignoreIfExists,
      boolean transientQueue, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean lastValue, boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filter, user, durable, temporary,
            ignoreIfExists,
            transientQueue, autoCreated, maxConsumers, purgeOnNoConsumers, exclusive, lastValue,
            autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      SimpleString queueName, SimpleString filterString, boolean durable, boolean temporary)
      throws Exception {
    return confluentAmqServer.createQueue(address, queueName, filterString, durable, temporary);
  }

  @Deprecated
  public Queue createQueue(
      AddressInfo addrInfo,
      SimpleString queueName,
      SimpleString filterString,
      SimpleString user, boolean durable, boolean temporary,
      boolean ignoreIfExists, boolean transientQueue, boolean autoCreated, int maxConsumers,
      boolean purgeOnNoConsumers, boolean exclusive, boolean groupRebalance, int groupBuckets,
      SimpleString groupFirstKey, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive,
      int consumersBeforeDispatch, long delayBeforeDispatch, boolean autoDelete,
      long autoDeleteDelay,
      long autoDeleteMessageCount, boolean autoCreateAddress, boolean configurationManaged,
      long ringSize) throws Exception {
    return confluentAmqServer
        .createQueue(addrInfo, queueName, filterString, user, durable, temporary, ignoreIfExists,
            transientQueue, autoCreated, maxConsumers, purgeOnNoConsumers, exclusive,
            groupRebalance,
            groupBuckets, groupFirstKey, lastValue, lastValueKey, nonDestructive,
            consumersBeforeDispatch, delayBeforeDispatch, autoDelete, autoDeleteDelay,
            autoDeleteMessageCount, autoCreateAddress, configurationManaged, ringSize);
  }

  @Override
  public Queue createQueue(QueueConfiguration queueConfiguration) throws Exception {
    return confluentAmqServer.createQueue(queueConfiguration);
  }

  @Override
  public Queue createQueue(QueueConfiguration queueConfiguration,
      boolean ignoreIfExists) throws Exception {
    return confluentAmqServer.createQueue(queueConfiguration, ignoreIfExists);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filterString,
      SimpleString user, boolean durable, boolean temporary, boolean ignoreIfExists,
      boolean transientQueue, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean groupRebalance, int groupBuckets, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount,
      boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filterString, user, durable, temporary,
            ignoreIfExists, transientQueue, autoCreated, maxConsumers, purgeOnNoConsumers,
            exclusive,
            groupRebalance, groupBuckets, lastValue, lastValueKey, nonDestructive,
            consumersBeforeDispatch, delayBeforeDispatch, autoDelete, autoDeleteDelay,
            autoDeleteMessageCount, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filterString,
      SimpleString user, boolean durable, boolean temporary, boolean ignoreIfExists,
      boolean transientQueue, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean groupRebalance, int groupBuckets,
      SimpleString groupFirstKey, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount,
      boolean autoCreateAddress) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filterString, user, durable, temporary,
            ignoreIfExists, transientQueue, autoCreated, maxConsumers, purgeOnNoConsumers,
            exclusive,
            groupRebalance, groupBuckets, groupFirstKey, lastValue, lastValueKey, nonDestructive,
            consumersBeforeDispatch, delayBeforeDispatch, autoDelete, autoDeleteDelay,
            autoDeleteMessageCount, autoCreateAddress);
  }

  @Override
  @Deprecated
  public Queue createQueue(SimpleString address,
      RoutingType routingType,
      SimpleString queueName, SimpleString filterString,
      SimpleString user, boolean durable, boolean temporary, boolean ignoreIfExists,
      boolean transientQueue, boolean autoCreated, int maxConsumers, boolean purgeOnNoConsumers,
      boolean exclusive, boolean groupRebalance, int groupBuckets,
      SimpleString groupFirstKey, boolean lastValue,
      SimpleString lastValueKey, boolean nonDestructive, int consumersBeforeDispatch,
      long delayBeforeDispatch, boolean autoDelete, long autoDeleteDelay,
      long autoDeleteMessageCount,
      boolean autoCreateAddress, long ringSize) throws Exception {
    return confluentAmqServer
        .createQueue(address, routingType, queueName, filterString, user, durable, temporary,
            ignoreIfExists, transientQueue, autoCreated, maxConsumers, purgeOnNoConsumers,
            exclusive,
            groupRebalance, groupBuckets, groupFirstKey, lastValue, lastValueKey, nonDestructive,
            consumersBeforeDispatch, delayBeforeDispatch, autoDelete, autoDeleteDelay,
            autoDeleteMessageCount, autoCreateAddress, ringSize);
  }

  @Override
  @Deprecated
  public Queue updateQueue(String name, RoutingType routingType,
      Integer maxConsumers, Boolean purgeOnNoConsumers) throws Exception {
    return confluentAmqServer.updateQueue(name, routingType, maxConsumers, purgeOnNoConsumers);
  }

  @Override
  @Deprecated
  public Queue updateQueue(String name, RoutingType routingType,
      Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive) throws Exception {
    return confluentAmqServer
        .updateQueue(name, routingType, maxConsumers, purgeOnNoConsumers, exclusive);
  }

  @Override
  @Deprecated
  public Queue updateQueue(String name, RoutingType routingType,
      Integer maxConsumers, Boolean purgeOnNoConsumers, Boolean exclusive, String user)
      throws Exception {
    return confluentAmqServer
        .updateQueue(name, routingType, maxConsumers, purgeOnNoConsumers, exclusive, user);
  }

  @Override
  @Deprecated
  public Queue updateQueue(String name, RoutingType routingType,
      String filterString, Integer maxConsumers, Boolean purgeOnNoConsumers,
      Boolean exclusive, Boolean groupRebalance, Integer groupBuckets,
      Boolean nonDestructive, Integer consumersBeforeDispatch, Long delayBeforeDispatch,
      String user) throws Exception {
    return confluentAmqServer
        .updateQueue(name, routingType, filterString, maxConsumers, purgeOnNoConsumers, exclusive,
            groupRebalance, groupBuckets, nonDestructive, consumersBeforeDispatch,
            delayBeforeDispatch,
            user);
  }

  @Override
  @Deprecated
  public Queue updateQueue(String name, RoutingType routingType,
      String filterString, Integer maxConsumers, Boolean purgeOnNoConsumers,
      Boolean exclusive, Boolean groupRebalance, Integer groupBuckets, String groupFirstKey,
      Boolean nonDestructive, Integer consumersBeforeDispatch, Long delayBeforeDispatch,
      String user) throws Exception {
    return confluentAmqServer
        .updateQueue(name, routingType, filterString, maxConsumers, purgeOnNoConsumers, exclusive,
            groupRebalance, groupBuckets, groupFirstKey, nonDestructive, consumersBeforeDispatch,
            delayBeforeDispatch, user);
  }

  @Override
  @Deprecated
  public Queue updateQueue(String name, RoutingType routingType,
      String filterString, Integer maxConsumers, Boolean purgeOnNoConsumers,
      Boolean exclusive, Boolean groupRebalance, Integer groupBuckets, String groupFirstKey,
      Boolean nonDestructive, Integer consumersBeforeDispatch, Long delayBeforeDispatch,
      String user, Long ringSize) throws Exception {
    return confluentAmqServer
        .updateQueue(name, routingType, filterString, maxConsumers, purgeOnNoConsumers, exclusive,
            groupRebalance, groupBuckets, groupFirstKey, nonDestructive, consumersBeforeDispatch,
            delayBeforeDispatch, user, ringSize);
  }

  @Override
  public Queue updateQueue(QueueConfiguration queueConfiguration) throws Exception {
    return confluentAmqServer.updateQueue(queueConfiguration);
  }

  @Override
  public void addProtocolManagerFactory(
      ProtocolManagerFactory factory) {
    confluentAmqServer.addProtocolManagerFactory(factory);
  }

  @Override
  public void removeProtocolManagerFactory(
      ProtocolManagerFactory factory) {
    confluentAmqServer.removeProtocolManagerFactory(factory);
  }

  @Override
  public ActiveMQServer createBackupServer(
      Configuration configuration) {
    return confluentAmqServer.createBackupServer(configuration);
  }

  @Override
  public void addScaledDownNode(SimpleString scaledDownNodeId) {
    confluentAmqServer.addScaledDownNode(scaledDownNodeId);
  }

  @Override
  public boolean hasScaledDown(SimpleString scaledDownNodeId) {
    return confluentAmqServer.hasScaledDown(scaledDownNodeId);
  }

  @Override
  public String getUptime() {
    return confluentAmqServer.getUptime();
  }

  @Override
  public long getUptimeMillis() {
    return confluentAmqServer.getUptimeMillis();
  }

  @Override
  public boolean addClientConnection(String clientId, boolean unique) {
    return confluentAmqServer.addClientConnection(clientId, unique);
  }

  @Override
  public void removeClientConnection(String clientId) {
    confluentAmqServer.removeClientConnection(clientId);
  }

  public Set<ActivateCallback> getActivateCallbacks() {
    return confluentAmqServer.getActivateCallbacks();
  }

  @Override
  public List<ActiveMQComponent> getExternalComponents() {
    return confluentAmqServer.getExternalComponents();
  }


  @Override
  public void asyncStop(Runnable callback) throws Exception {
    confluentAmqServer.asyncStop(callback);
  }


}
