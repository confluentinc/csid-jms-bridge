/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import org.apache.activemq.artemis.api.core.BroadcastGroupConfiguration;
import org.apache.activemq.artemis.api.core.DiscoveryGroupConfiguration;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.TransportConfiguration;
import org.apache.activemq.artemis.core.config.BridgeConfiguration;
import org.apache.activemq.artemis.core.config.ClusterConnectionConfiguration;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.ConnectorServiceConfiguration;
import org.apache.activemq.artemis.core.config.CoreAddressConfiguration;
import org.apache.activemq.artemis.core.config.DivertConfiguration;
import org.apache.activemq.artemis.core.config.FederationConfiguration;
import org.apache.activemq.artemis.core.config.HAPolicyConfiguration;
import org.apache.activemq.artemis.core.config.StoreConfiguration;
import org.apache.activemq.artemis.core.config.WildcardConfiguration;
import org.apache.activemq.artemis.core.config.impl.ConfigurationImpl;
import org.apache.activemq.artemis.core.security.Role;
import org.apache.activemq.artemis.core.server.JournalType;
import org.apache.activemq.artemis.core.server.SecuritySettingPlugin;
import org.apache.activemq.artemis.core.server.group.impl.GroupingHandlerConfiguration;
import org.apache.activemq.artemis.core.server.metrics.ActiveMQMetricsPlugin;
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
import org.apache.activemq.artemis.core.settings.impl.AddressSettings;
import org.apache.activemq.artemis.core.settings.impl.ResourceLimitSettings;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzerPolicy;

@SuppressWarnings("deprecation")
public class JmsBridgeConfiguration implements Configuration {

  private final Configuration delegate;
  private final Properties jmsBridgeProperties;

  public JmsBridgeConfiguration(Configuration delegate, Properties jmsBridgeProperties) {
    this.delegate = delegate;
    this.jmsBridgeProperties = jmsBridgeProperties;
  }

  public Properties getJmsBridgeProperties() {
    return jmsBridgeProperties;
  }

  @Override
  public String getName() {
    return delegate.getName();
  }

  @Override
  public Configuration setName(String name) {
    return delegate.setName(name);
  }

  @Override
  public Configuration setSystemPropertyPrefix(String systemPropertyPrefix) {
    return delegate.setSystemPropertyPrefix(systemPropertyPrefix);
  }

  @Override
  public String getSystemPropertyPrefix() {
    return delegate.getSystemPropertyPrefix();
  }

  @Override
  public Configuration parseSystemProperties() throws Exception {
    return delegate.parseSystemProperties();
  }

  @Override
  public Configuration parseSystemProperties(Properties properties) throws Exception {
    return delegate.parseSystemProperties(properties);
  }

  @Override
  public boolean isCriticalAnalyzer() {
    return delegate.isCriticalAnalyzer();
  }

  @SuppressWarnings("checkstyle:ParameterName")
  @Override
  public Configuration setCriticalAnalyzer(boolean CriticalAnalyzer) {
    return delegate.setCriticalAnalyzer(CriticalAnalyzer);
  }

  @Override
  public long getCriticalAnalyzerTimeout() {
    return delegate.getCriticalAnalyzerTimeout();
  }

  @Override
  public Configuration setCriticalAnalyzerTimeout(long timeout) {
    return delegate.setCriticalAnalyzerTimeout(timeout);
  }

  @Override
  public long getCriticalAnalyzerCheckPeriod() {
    return delegate.getCriticalAnalyzerCheckPeriod();
  }

  @Override
  public Configuration setCriticalAnalyzerCheckPeriod(long checkPeriod) {
    return delegate.setCriticalAnalyzerCheckPeriod(checkPeriod);
  }

  @Override
  public CriticalAnalyzerPolicy getCriticalAnalyzerPolicy() {
    return delegate.getCriticalAnalyzerPolicy();
  }

  @Override
  public Configuration setCriticalAnalyzerPolicy(
      CriticalAnalyzerPolicy policy) {
    return delegate.setCriticalAnalyzerPolicy(policy);
  }

  @Override
  public boolean isClustered() {
    return delegate.isClustered();
  }

  @Override
  public boolean isPersistDeliveryCountBeforeDelivery() {
    return delegate.isPersistDeliveryCountBeforeDelivery();
  }

  @Override
  public Configuration setPersistDeliveryCountBeforeDelivery(
      boolean persistDeliveryCountBeforeDelivery) {
    return delegate.setPersistDeliveryCountBeforeDelivery(persistDeliveryCountBeforeDelivery);
  }

  @Override
  public boolean isPersistenceEnabled() {
    return delegate.isPersistenceEnabled();
  }

  @Override
  public Configuration setPersistenceEnabled(boolean enable) {
    return delegate.setPersistenceEnabled(enable);
  }

  @Override
  public boolean isJournalDatasync() {
    return delegate.isJournalDatasync();
  }

  @Override
  public Configuration setJournalDatasync(boolean enable) {
    return delegate.setJournalDatasync(enable);
  }

  @Override
  public Map<String, ResourceLimitSettings> getResourceLimitSettings() {
    return delegate.getResourceLimitSettings();
  }

  @Override
  public Configuration setResourceLimitSettings(
      Map<String, ResourceLimitSettings> resourceLimitSettings) {
    return delegate.setResourceLimitSettings(resourceLimitSettings);
  }

  @Override
  public Configuration addResourceLimitSettings(
      ResourceLimitSettings resourceLimitSettings) {
    return delegate.addResourceLimitSettings(resourceLimitSettings);
  }

  @Override
  public long getFileDeployerScanPeriod() {
    return delegate.getFileDeployerScanPeriod();
  }

  @Override
  public Configuration setFileDeployerScanPeriod(long period) {
    return delegate.setFileDeployerScanPeriod(period);
  }

  @Override
  public int getThreadPoolMaxSize() {
    return delegate.getThreadPoolMaxSize();
  }

  @Override
  public Configuration setThreadPoolMaxSize(int maxSize) {
    return delegate.setThreadPoolMaxSize(maxSize);
  }

  @Override
  public int getScheduledThreadPoolMaxSize() {
    return delegate.getScheduledThreadPoolMaxSize();
  }

  @Override
  public Configuration setScheduledThreadPoolMaxSize(int maxSize) {
    return delegate.setScheduledThreadPoolMaxSize(maxSize);
  }

  @Override
  public long getSecurityInvalidationInterval() {
    return delegate.getSecurityInvalidationInterval();
  }

  @Override
  public Configuration setSecurityInvalidationInterval(long interval) {
    return delegate.setSecurityInvalidationInterval(interval);
  }

  @Override
  public boolean isSecurityEnabled() {
    return delegate.isSecurityEnabled();
  }

  @Override
  public Configuration setSecurityEnabled(boolean enabled) {
    return delegate.setSecurityEnabled(enabled);
  }

  @Override
  public boolean isGracefulShutdownEnabled() {
    return delegate.isGracefulShutdownEnabled();
  }

  @Override
  public Configuration setGracefulShutdownEnabled(boolean enabled) {
    return delegate.setGracefulShutdownEnabled(enabled);
  }

  @Override
  public long getGracefulShutdownTimeout() {
    return delegate.getGracefulShutdownTimeout();
  }

  @Override
  public Configuration setGracefulShutdownTimeout(long timeout) {
    return delegate.setGracefulShutdownTimeout(timeout);
  }

  @Override
  public boolean isJMXManagementEnabled() {
    return delegate.isJMXManagementEnabled();
  }

  @Override
  public Configuration setJMXManagementEnabled(boolean enabled) {
    return delegate.setJMXManagementEnabled(enabled);
  }

  @Override
  public String getJMXDomain() {
    return delegate.getJMXDomain();
  }

  @Override
  public Configuration setJMXDomain(String domain) {
    return delegate.setJMXDomain(domain);
  }

  @Override
  public boolean isJMXUseBrokerName() {
    return delegate.isJMXUseBrokerName();
  }

  @Override
  public ConfigurationImpl setJMXUseBrokerName(
      boolean jmxUseBrokerName) {
    return delegate.setJMXUseBrokerName(jmxUseBrokerName);
  }

  @Override
  public List<String> getIncomingInterceptorClassNames() {
    return delegate.getIncomingInterceptorClassNames();
  }

  @Override
  public List<String> getOutgoingInterceptorClassNames() {
    return delegate.getOutgoingInterceptorClassNames();
  }

  @Override
  public Configuration setIncomingInterceptorClassNames(List<String> interceptors) {
    return delegate.setIncomingInterceptorClassNames(interceptors);
  }

  @Override
  public Configuration setOutgoingInterceptorClassNames(List<String> interceptors) {
    return delegate.setOutgoingInterceptorClassNames(interceptors);
  }

  @Override
  public long getConnectionTTLOverride() {
    return delegate.getConnectionTTLOverride();
  }

  @Override
  public Configuration setConnectionTTLOverride(long ttl) {
    return delegate.setConnectionTTLOverride(ttl);
  }

  @Override
  public boolean isAmqpUseCoreSubscriptionNaming() {
    return delegate.isAmqpUseCoreSubscriptionNaming();
  }

  @Override
  public Configuration setAmqpUseCoreSubscriptionNaming(boolean amqpUseCoreSubscriptionNaming) {
    return delegate.setAmqpUseCoreSubscriptionNaming(amqpUseCoreSubscriptionNaming);
  }

  @Override
  @Deprecated
  public boolean isAsyncConnectionExecutionEnabled() {
    return delegate.isAsyncConnectionExecutionEnabled();
  }

  @Override
  @Deprecated
  public Configuration setEnabledAsyncConnectionExecution(boolean enabled) {
    return delegate.setEnabledAsyncConnectionExecution(enabled);
  }

  @Override
  public Set<TransportConfiguration> getAcceptorConfigurations() {
    return delegate.getAcceptorConfigurations();
  }

  @Override
  public Configuration setAcceptorConfigurations(
      Set<TransportConfiguration> infos) {
    return delegate.setAcceptorConfigurations(infos);
  }

  @Override
  public Configuration addAcceptorConfiguration(
      TransportConfiguration infos) {
    return delegate.addAcceptorConfiguration(infos);
  }

  @Override
  public Configuration addAcceptorConfiguration(String name, String uri) throws Exception {
    return delegate.addAcceptorConfiguration(name, uri);
  }

  @Override
  public Configuration clearAcceptorConfigurations() {
    return delegate.clearAcceptorConfigurations();
  }

  @Override
  public Map<String, TransportConfiguration> getConnectorConfigurations() {
    return delegate.getConnectorConfigurations();
  }

  @Override
  public Configuration setConnectorConfigurations(
      Map<String, TransportConfiguration> infos) {
    return delegate.setConnectorConfigurations(infos);
  }

  @Override
  public Configuration addConnectorConfiguration(String key,
      TransportConfiguration info) {
    return delegate.addConnectorConfiguration(key, info);
  }

  @Override
  public Configuration addConnectorConfiguration(String name, String uri) throws Exception {
    return delegate.addConnectorConfiguration(name, uri);
  }

  @Override
  public Configuration clearConnectorConfigurations() {
    return delegate.clearConnectorConfigurations();
  }

  @Override
  public List<BroadcastGroupConfiguration> getBroadcastGroupConfigurations() {
    return delegate.getBroadcastGroupConfigurations();
  }

  @Override
  public Configuration setBroadcastGroupConfigurations(
      List<BroadcastGroupConfiguration> configs) {
    return delegate.setBroadcastGroupConfigurations(configs);
  }

  @Override
  public Configuration addBroadcastGroupConfiguration(
      BroadcastGroupConfiguration config) {
    return delegate.addBroadcastGroupConfiguration(config);
  }

  @Override
  public Map<String, DiscoveryGroupConfiguration> getDiscoveryGroupConfigurations() {
    return delegate.getDiscoveryGroupConfigurations();
  }

  @Override
  public Configuration setDiscoveryGroupConfigurations(
      Map<String, DiscoveryGroupConfiguration> configs) {
    return delegate.setDiscoveryGroupConfigurations(configs);
  }

  @Override
  public Configuration addDiscoveryGroupConfiguration(String key,
      DiscoveryGroupConfiguration discoveryGroupConfiguration) {
    return delegate.addDiscoveryGroupConfiguration(key, discoveryGroupConfiguration);
  }

  @Override
  public GroupingHandlerConfiguration getGroupingHandlerConfiguration() {
    return delegate.getGroupingHandlerConfiguration();
  }

  @Override
  public Configuration setGroupingHandlerConfiguration(
      GroupingHandlerConfiguration groupingHandlerConfiguration) {
    return delegate.setGroupingHandlerConfiguration(groupingHandlerConfiguration);
  }

  @Override
  public List<BridgeConfiguration> getBridgeConfigurations() {
    return delegate.getBridgeConfigurations();
  }

  @Override
  public Configuration setBridgeConfigurations(
      List<BridgeConfiguration> configs) {
    return delegate.setBridgeConfigurations(configs);
  }

  @Override
  public List<DivertConfiguration> getDivertConfigurations() {
    return delegate.getDivertConfigurations();
  }

  @Override
  public Configuration setDivertConfigurations(
      List<DivertConfiguration> configs) {
    return delegate.setDivertConfigurations(configs);
  }

  @Override
  public Configuration addDivertConfiguration(
      DivertConfiguration config) {
    return delegate.addDivertConfiguration(config);
  }

  @Override
  public List<ClusterConnectionConfiguration> getClusterConfigurations() {
    return delegate.getClusterConfigurations();
  }

  @Override
  public Configuration setClusterConfigurations(
      List<ClusterConnectionConfiguration> configs) {
    return delegate.setClusterConfigurations(configs);
  }

  @Override
  public Configuration addClusterConfiguration(
      ClusterConnectionConfiguration config) {
    return delegate.addClusterConfiguration(config);
  }

  @Override
  public ClusterConnectionConfiguration addClusterConfiguration(
      String name, String uri) throws Exception {
    return delegate.addClusterConfiguration(name, uri);
  }

  @Override
  public Configuration clearClusterConfigurations() {
    return delegate.clearClusterConfigurations();
  }

  @Override
  @Deprecated
  public List<org.apache.activemq.artemis.core.config.CoreQueueConfiguration> getQueueConfigurations() {
    return delegate.getQueueConfigurations();
  }

  @Override
  public List<QueueConfiguration> getQueueConfigs() {
    return delegate.getQueueConfigs();
  }

  @Override
  @Deprecated
  public Configuration setQueueConfigurations(
      List<org.apache.activemq.artemis.core.config.CoreQueueConfiguration> configs) {
    return delegate.setQueueConfigurations(configs);
  }

  @Override
  public Configuration setQueueConfigs(
      List<QueueConfiguration> configs) {
    return delegate.setQueueConfigs(configs);
  }


  @Override
  @Deprecated
  public Configuration addQueueConfiguration(
      org.apache.activemq.artemis.core.config.CoreQueueConfiguration config) {
    return delegate.addQueueConfiguration(config);
  }

  @Override
  public Configuration addQueueConfiguration(
      QueueConfiguration config) {
    return delegate.addQueueConfiguration(config);
  }

  @Override
  public List<CoreAddressConfiguration> getAddressConfigurations() {
    return delegate.getAddressConfigurations();
  }

  @Override
  public Configuration setAddressConfigurations(
      List<CoreAddressConfiguration> configs) {
    return delegate.setAddressConfigurations(configs);
  }

  @Override
  public Configuration addAddressConfiguration(
      CoreAddressConfiguration config) {
    return delegate.addAddressConfiguration(config);
  }

  @Override
  public SimpleString getManagementAddress() {
    return delegate.getManagementAddress();
  }

  @Override
  public Configuration setManagementAddress(SimpleString address) {
    return delegate.setManagementAddress(address);
  }

  @Override
  public SimpleString getManagementNotificationAddress() {
    return delegate.getManagementNotificationAddress();
  }

  @Override
  public Configuration setManagementNotificationAddress(
      SimpleString address) {
    return delegate.setManagementNotificationAddress(address);
  }

  @Override
  public String getClusterUser() {
    return delegate.getClusterUser();
  }

  @Override
  public Configuration setClusterUser(String user) {
    return delegate.setClusterUser(user);
  }

  @Override
  public String getClusterPassword() {
    return delegate.getClusterPassword();
  }

  @Override
  public Configuration setClusterPassword(String password) {
    return delegate.setClusterPassword(password);
  }

  @Override
  public int getIDCacheSize() {
    return delegate.getIDCacheSize();
  }

  @Override
  public Configuration setIDCacheSize(int idCacheSize) {
    return delegate.setIDCacheSize(idCacheSize);
  }

  @Override
  public boolean isPersistIDCache() {
    return delegate.isPersistIDCache();
  }

  @Override
  public Configuration setPersistIDCache(boolean persist) {
    return delegate.setPersistIDCache(persist);
  }

  @Override
  public String getBindingsDirectory() {
    return delegate.getBindingsDirectory();
  }

  @Override
  public File getBindingsLocation() {
    return delegate.getBindingsLocation();
  }

  @Override
  public Configuration setBindingsDirectory(String dir) {
    return delegate.setBindingsDirectory(dir);
  }

  @Override
  public int getPageMaxConcurrentIO() {
    return delegate.getPageMaxConcurrentIO();
  }

  @Override
  public Configuration setPageMaxConcurrentIO(int maxIO) {
    return delegate.setPageMaxConcurrentIO(maxIO);
  }

  @Override
  public boolean isReadWholePage() {
    return delegate.isReadWholePage();
  }

  @Override
  public Configuration setReadWholePage(boolean read) {
    return delegate.setReadWholePage(read);
  }

  @Override
  public String getJournalDirectory() {
    return delegate.getJournalDirectory();
  }

  @Override
  public File getJournalLocation() {
    return delegate.getJournalLocation();
  }

  @Override
  public File getNodeManagerLockLocation() {
    return delegate.getNodeManagerLockLocation();
  }

  @Override
  public Configuration setNodeManagerLockDirectory(String dir) {
    return delegate.setNodeManagerLockDirectory(dir);
  }

  @Override
  public Configuration setJournalDirectory(String dir) {
    return delegate.setJournalDirectory(dir);
  }

  @Override
  public JournalType getJournalType() {
    return delegate.getJournalType();
  }

  @Override
  public Configuration setJournalType(JournalType type) {
    return delegate.setJournalType(type);
  }

  @Override
  public boolean isJournalSyncTransactional() {
    return delegate.isJournalSyncTransactional();
  }

  @Override
  public Configuration setJournalSyncTransactional(boolean sync) {
    return delegate.setJournalSyncTransactional(sync);
  }

  @Override
  public boolean isJournalSyncNonTransactional() {
    return delegate.isJournalSyncNonTransactional();
  }

  @Override
  public Configuration setJournalSyncNonTransactional(boolean sync) {
    return delegate.setJournalSyncNonTransactional(sync);
  }

  @Override
  public int getJournalFileSize() {
    return delegate.getJournalFileSize();
  }

  @Override
  public Configuration setJournalFileSize(int size) {
    return delegate.setJournalFileSize(size);
  }

  @Override
  public int getJournalCompactMinFiles() {
    return delegate.getJournalCompactMinFiles();
  }

  @Override
  public Configuration setJournalCompactMinFiles(int minFiles) {
    return delegate.setJournalCompactMinFiles(minFiles);
  }

  @Override
  public int getJournalPoolFiles() {
    return delegate.getJournalPoolFiles();
  }

  @Override
  public Configuration setJournalPoolFiles(int poolSize) {
    return delegate.setJournalPoolFiles(poolSize);
  }

  @Override
  public int getJournalCompactPercentage() {
    return delegate.getJournalCompactPercentage();
  }

  @Override
  public int getJournalFileOpenTimeout() {
    return delegate.getJournalFileOpenTimeout();
  }

  @Override
  public Configuration setJournalFileOpenTimeout(int journalFileOpenTimeout) {
    return delegate.setJournalFileOpenTimeout(journalFileOpenTimeout);
  }

  @Override
  public Configuration setJournalCompactPercentage(int percentage) {
    return delegate.setJournalCompactPercentage(percentage);
  }

  @Override
  public int getJournalMinFiles() {
    return delegate.getJournalMinFiles();
  }

  @Override
  public Configuration setJournalMinFiles(int files) {
    return delegate.setJournalMinFiles(files);
  }

  @Override
  public int getJournalMaxIO_AIO() {
    return delegate.getJournalMaxIO_AIO();
  }

  @Override
  public Configuration setJournalMaxIO_AIO(int journalMaxIO) {
    return delegate.setJournalMaxIO_AIO(journalMaxIO);
  }

  @Override
  public int getJournalBufferTimeout_AIO() {
    return delegate.getJournalBufferTimeout_AIO();
  }

  @Override
  public Configuration setJournalBufferTimeout_AIO(int journalBufferTimeout) {
    return delegate.setJournalBufferTimeout_AIO(journalBufferTimeout);
  }

  @Override
  public Integer getJournalDeviceBlockSize() {
    return delegate.getJournalDeviceBlockSize();
  }

  @Override
  public Configuration setJournalDeviceBlockSize(Integer deviceBlockSize) {
    return delegate.setJournalDeviceBlockSize(deviceBlockSize);
  }

  @Override
  public int getJournalBufferSize_AIO() {
    return delegate.getJournalBufferSize_AIO();
  }

  @Override
  public Configuration setJournalBufferSize_AIO(int journalBufferSize) {
    return delegate.setJournalBufferSize_AIO(journalBufferSize);
  }

  @Override
  public int getJournalMaxIO_NIO() {
    return delegate.getJournalMaxIO_NIO();
  }

  @Override
  public Configuration setJournalMaxIO_NIO(int journalMaxIO) {
    return delegate.setJournalMaxIO_NIO(journalMaxIO);
  }

  @Override
  public int getJournalBufferTimeout_NIO() {
    return delegate.getJournalBufferTimeout_NIO();
  }

  @Override
  public Configuration setJournalBufferTimeout_NIO(int journalBufferTimeout) {
    return delegate.setJournalBufferTimeout_NIO(journalBufferTimeout);
  }

  @Override
  public int getJournalBufferSize_NIO() {
    return delegate.getJournalBufferSize_NIO();
  }

  @Override
  public Configuration setJournalBufferSize_NIO(int journalBufferSize) {
    return delegate.setJournalBufferSize_NIO(journalBufferSize);
  }

  @Override
  public boolean isCreateBindingsDir() {
    return delegate.isCreateBindingsDir();
  }

  @Override
  public Configuration setCreateBindingsDir(boolean create) {
    return delegate.setCreateBindingsDir(create);
  }

  @Override
  public boolean isCreateJournalDir() {
    return delegate.isCreateJournalDir();
  }

  @Override
  public Configuration setCreateJournalDir(boolean create) {
    return delegate.setCreateJournalDir(create);
  }

  @Override
  public boolean isLogJournalWriteRate() {
    return delegate.isLogJournalWriteRate();
  }

  @Override
  public Configuration setLogJournalWriteRate(boolean rate) {
    return delegate.setLogJournalWriteRate(rate);
  }

  @Override
  public long getServerDumpInterval() {
    return delegate.getServerDumpInterval();
  }

  @Override
  public Configuration setServerDumpInterval(long interval) {
    return delegate.setServerDumpInterval(interval);
  }

  @Override
  public int getMemoryWarningThreshold() {
    return delegate.getMemoryWarningThreshold();
  }

  @Override
  public Configuration setMemoryWarningThreshold(int memoryWarningThreshold) {
    return delegate.setMemoryWarningThreshold(memoryWarningThreshold);
  }

  @Override
  public long getMemoryMeasureInterval() {
    return delegate.getMemoryMeasureInterval();
  }

  @Override
  public Configuration setMemoryMeasureInterval(long memoryMeasureInterval) {
    return delegate.setMemoryMeasureInterval(memoryMeasureInterval);
  }

  @Override
  public String getPagingDirectory() {
    return delegate.getPagingDirectory();
  }

  @Override
  public Configuration setPagingDirectory(String dir) {
    return delegate.setPagingDirectory(dir);
  }

  @Override
  public File getPagingLocation() {
    return delegate.getPagingLocation();
  }

  @Override
  public String getLargeMessagesDirectory() {
    return delegate.getLargeMessagesDirectory();
  }

  @Override
  public File getLargeMessagesLocation() {
    return delegate.getLargeMessagesLocation();
  }

  @Override
  public Configuration setLargeMessagesDirectory(String directory) {
    return delegate.setLargeMessagesDirectory(directory);
  }

  @Override
  public boolean isWildcardRoutingEnabled() {
    return delegate.isWildcardRoutingEnabled();
  }

  @Override
  public Configuration setWildcardRoutingEnabled(boolean enabled) {
    return delegate.setWildcardRoutingEnabled(enabled);
  }

  @Override
  public WildcardConfiguration getWildcardConfiguration() {
    return delegate.getWildcardConfiguration();
  }

  @Override
  public Configuration setWildCardConfiguration(
      WildcardConfiguration wildcardConfiguration) {
    return delegate.setWildCardConfiguration(wildcardConfiguration);
  }

  @Override
  public long getTransactionTimeout() {
    return delegate.getTransactionTimeout();
  }

  @Override
  public Configuration setTransactionTimeout(long timeout) {
    return delegate.setTransactionTimeout(timeout);
  }

  @Override
  public boolean isMessageCounterEnabled() {
    return delegate.isMessageCounterEnabled();
  }

  @Override
  public Configuration setMessageCounterEnabled(boolean enabled) {
    return delegate.setMessageCounterEnabled(enabled);
  }

  @Override
  public long getMessageCounterSamplePeriod() {
    return delegate.getMessageCounterSamplePeriod();
  }

  @Override
  public Configuration setMessageCounterSamplePeriod(long period) {
    return delegate.setMessageCounterSamplePeriod(period);
  }

  @Override
  public int getMessageCounterMaxDayHistory() {
    return delegate.getMessageCounterMaxDayHistory();
  }

  @Override
  public Configuration setMessageCounterMaxDayHistory(int maxDayHistory) {
    return delegate.setMessageCounterMaxDayHistory(maxDayHistory);
  }

  @Override
  public long getTransactionTimeoutScanPeriod() {
    return delegate.getTransactionTimeoutScanPeriod();
  }

  @Override
  public Configuration setTransactionTimeoutScanPeriod(long period) {
    return delegate.setTransactionTimeoutScanPeriod(period);
  }

  @Override
  public long getMessageExpiryScanPeriod() {
    return delegate.getMessageExpiryScanPeriod();
  }

  @Override
  public Configuration setMessageExpiryScanPeriod(long messageExpiryScanPeriod) {
    return delegate.setMessageExpiryScanPeriod(messageExpiryScanPeriod);
  }

  @Override
  @Deprecated
  public int getMessageExpiryThreadPriority() {
    return delegate.getMessageExpiryThreadPriority();
  }

  @Override
  @Deprecated
  public Configuration setMessageExpiryThreadPriority(int messageExpiryThreadPriority) {
    return delegate.setMessageExpiryThreadPriority(messageExpiryThreadPriority);
  }

  @Override
  public long getAddressQueueScanPeriod() {
    return delegate.getAddressQueueScanPeriod();
  }

  @Override
  public Configuration setAddressQueueScanPeriod(long addressQueueScanPeriod) {
    return delegate.setAddressQueueScanPeriod(addressQueueScanPeriod);
  }

  @Override
  public Map<String, AddressSettings> getAddressesSettings() {
    return delegate.getAddressesSettings();
  }

  @Override
  public Configuration setAddressesSettings(
      Map<String, AddressSettings> addressesSettings) {
    return delegate.setAddressesSettings(addressesSettings);
  }

  @Override
  public Configuration addAddressesSetting(String key,
      AddressSettings addressesSetting) {
    return delegate.addAddressesSetting(key, addressesSetting);
  }

  @Override
  public Configuration clearAddressesSettings() {
    return delegate.clearAddressesSettings();
  }

  @Override
  public Configuration setSecurityRoles(
      Map<String, Set<Role>> roles) {
    return delegate.setSecurityRoles(roles);
  }

  @Override
  public Map<String, Set<Role>> getSecurityRoles() {
    return delegate.getSecurityRoles();
  }

  @Override
  public Configuration addSecurityRoleNameMapping(String internalRole,
      Set<String> externalRoles) {
    return delegate.addSecurityRoleNameMapping(internalRole, externalRoles);
  }

  @Override
  public Map<String, Set<String>> getSecurityRoleNameMappings() {
    return delegate.getSecurityRoleNameMappings();
  }

  @Override
  public Configuration putSecurityRoles(String match,
      Set<Role> roles) {
    return delegate.putSecurityRoles(match, roles);
  }

  @Override
  public Configuration setConnectorServiceConfigurations(
      List<ConnectorServiceConfiguration> configs) {
    return delegate.setConnectorServiceConfigurations(configs);
  }

  @Override
  public Configuration addConnectorServiceConfiguration(
      ConnectorServiceConfiguration config) {
    return delegate.addConnectorServiceConfiguration(config);
  }

  @Override
  public Configuration setSecuritySettingPlugins(
      List<SecuritySettingPlugin> plugins) {
    return delegate.setSecuritySettingPlugins(plugins);
  }

  @Override
  public Configuration addSecuritySettingPlugin(
      SecuritySettingPlugin plugin) {
    return delegate.addSecuritySettingPlugin(plugin);
  }

  @Override
  public Configuration setMetricsPlugin(
      ActiveMQMetricsPlugin plugin) {
    return delegate.setMetricsPlugin(plugin);
  }

  @Override
  public List<ConnectorServiceConfiguration> getConnectorServiceConfigurations() {
    return delegate.getConnectorServiceConfigurations();
  }

  @Override
  public List<SecuritySettingPlugin> getSecuritySettingPlugins() {
    return delegate.getSecuritySettingPlugins();
  }

  @Override
  public ActiveMQMetricsPlugin getMetricsPlugin() {
    return delegate.getMetricsPlugin();
  }

  @Override
  public Configuration setPasswordCodec(String codec) {
    return delegate.setPasswordCodec(codec);
  }

  @Override
  public String getPasswordCodec() {
    return delegate.getPasswordCodec();
  }

  @Override
  public Configuration setMaskPassword(Boolean maskPassword) {
    return delegate.setMaskPassword(maskPassword);
  }

  @Override
  public Boolean isMaskPassword() {
    return delegate.isMaskPassword();
  }

  @Override
  public Configuration setResolveProtocols(boolean resolveProtocols) {
    return delegate.setResolveProtocols(resolveProtocols);
  }

  @Override
  public TransportConfiguration[] getTransportConfigurations(
      String... connectorNames) {
    return delegate.getTransportConfigurations(connectorNames);
  }

  @Override
  public TransportConfiguration[] getTransportConfigurations(
      List<String> connectorNames) {
    return delegate.getTransportConfigurations(connectorNames);
  }

  @Override
  public boolean isResolveProtocols() {
    return delegate.isResolveProtocols();
  }

  @Override
  public Configuration copy() throws Exception {
    return delegate.copy();
  }

  @Override
  public Configuration setJournalLockAcquisitionTimeout(long journalLockAcquisitionTimeout) {
    return delegate.setJournalLockAcquisitionTimeout(journalLockAcquisitionTimeout);
  }

  @Override
  public long getJournalLockAcquisitionTimeout() {
    return delegate.getJournalLockAcquisitionTimeout();
  }

  @Override
  public HAPolicyConfiguration getHAPolicyConfiguration() {
    return delegate.getHAPolicyConfiguration();
  }

  @Override
  public Configuration setHAPolicyConfiguration(
      HAPolicyConfiguration haPolicyConfiguration) {
    return delegate.setHAPolicyConfiguration(haPolicyConfiguration);
  }

  @Override
  public void setBrokerInstance(File directory) {
    delegate.setBrokerInstance(directory);
  }

  @Override
  public File getBrokerInstance() {
    return delegate.getBrokerInstance();
  }

  @Override
  public boolean isJDBC() {
    return delegate.isJDBC();
  }

  @Override
  public StoreConfiguration getStoreConfiguration() {
    return delegate.getStoreConfiguration();
  }

  @Override
  public Configuration setStoreConfiguration(
      StoreConfiguration storeConfiguration) {
    return delegate.setStoreConfiguration(storeConfiguration);
  }

  @Override
  public boolean isPopulateValidatedUser() {
    return delegate.isPopulateValidatedUser();
  }

  @Override
  public Configuration setPopulateValidatedUser(boolean populateValidatedUser) {
    return delegate.setPopulateValidatedUser(populateValidatedUser);
  }

  @Override
  public boolean isRejectEmptyValidatedUser() {
    return delegate.isRejectEmptyValidatedUser();
  }

  @Override
  public Configuration setRejectEmptyValidatedUser(boolean rejectEmptyValidatedUser) {
    return delegate.setRejectEmptyValidatedUser(rejectEmptyValidatedUser);
  }

  @Override
  public String debugConnectors() {
    return delegate.debugConnectors();
  }

  @Override
  public Configuration setConnectionTtlCheckInterval(long connectionTtlCheckInterval) {
    return delegate.setConnectionTtlCheckInterval(connectionTtlCheckInterval);
  }

  @Override
  public long getConnectionTtlCheckInterval() {
    return delegate.getConnectionTtlCheckInterval();
  }

  @Override
  public URL getConfigurationUrl() {
    return delegate.getConfigurationUrl();
  }

  @Override
  public Configuration setConfigurationUrl(URL configurationUrl) {
    return delegate.setConfigurationUrl(configurationUrl);
  }

  @Override
  public long getConfigurationFileRefreshPeriod() {
    return delegate.getConfigurationFileRefreshPeriod();
  }

  @Override
  public Configuration setConfigurationFileRefreshPeriod(long configurationFileRefreshPeriod) {
    return delegate.setConfigurationFileRefreshPeriod(configurationFileRefreshPeriod);
  }

  @Override
  public long getGlobalMaxSize() {
    return delegate.getGlobalMaxSize();
  }

  @Override
  public Configuration setGlobalMaxSize(long globalMaxSize) {
    return delegate.setGlobalMaxSize(globalMaxSize);
  }

  @Override
  public int getMaxDiskUsage() {
    return delegate.getMaxDiskUsage();
  }

  @Override
  public Configuration setMaxDiskUsage(int maxDiskUsage) {
    return delegate.setMaxDiskUsage(maxDiskUsage);
  }

  @Override
  public ConfigurationImpl setInternalNamingPrefix(
      String internalNamingPrefix) {
    return delegate.setInternalNamingPrefix(internalNamingPrefix);
  }

  @Override
  public Configuration setDiskScanPeriod(int diskScanPeriod) {
    return delegate.setDiskScanPeriod(diskScanPeriod);
  }

  @Override
  public int getDiskScanPeriod() {
    return delegate.getDiskScanPeriod();
  }

  @Override
  public Configuration setNetworkCheckList(String list) {
    return delegate.setNetworkCheckList(list);
  }

  @Override
  public String getNetworkCheckList() {
    return delegate.getNetworkCheckList();
  }

  @Override
  public Configuration setNetworkCheckURLList(String uris) {
    return delegate.setNetworkCheckURLList(uris);
  }

  @Override
  public String getNetworkCheckURLList() {
    return delegate.getNetworkCheckURLList();
  }

  @Override
  public Configuration setNetworkCheckPeriod(long period) {
    return delegate.setNetworkCheckPeriod(period);
  }

  @Override
  public long getNetworkCheckPeriod() {
    return delegate.getNetworkCheckPeriod();
  }

  @Override
  public Configuration setNetworkCheckTimeout(int timeout) {
    return delegate.setNetworkCheckTimeout(timeout);
  }

  @Override
  public int getNetworkCheckTimeout() {
    return delegate.getNetworkCheckTimeout();
  }

  @Override
  public Configuration setNetworCheckNIC(String nic) {
    return delegate.setNetworCheckNIC(nic);
  }

  @Override
  public String getNetworkCheckNIC() {
    return delegate.getNetworkCheckNIC();
  }

  @Override
  public String getNetworkCheckPingCommand() {
    return delegate.getNetworkCheckPingCommand();
  }

  @Override
  public Configuration setNetworkCheckPingCommand(String command) {
    return delegate.setNetworkCheckPingCommand(command);
  }

  @Override
  public String getNetworkCheckPing6Command() {
    return delegate.getNetworkCheckPing6Command();
  }

  @Override
  public Configuration setNetworkCheckPing6Command(String command) {
    return delegate.setNetworkCheckPing6Command(command);
  }

  @Override
  public String getInternalNamingPrefix() {
    return delegate.getInternalNamingPrefix();
  }

  @Override
  public int getPageSyncTimeout() {
    return delegate.getPageSyncTimeout();
  }

  @Override
  public Configuration setPageSyncTimeout(int pageSyncTimeout) {
    return delegate.setPageSyncTimeout(pageSyncTimeout);
  }

  @Override
  public void registerBrokerPlugins(
      List<ActiveMQServerBasePlugin> plugins) {
    delegate.registerBrokerPlugins(plugins);
  }

  @Override
  public void registerBrokerPlugin(
      ActiveMQServerBasePlugin plugin) {
    delegate.registerBrokerPlugin(plugin);
  }

  @Override
  public void unRegisterBrokerPlugin(
      ActiveMQServerBasePlugin plugin) {
    delegate.unRegisterBrokerPlugin(plugin);
  }

  @Override
  public List<ActiveMQServerBasePlugin> getBrokerPlugins() {
    return delegate.getBrokerPlugins();
  }

  @Override
  public List<ActiveMQServerConnectionPlugin> getBrokerConnectionPlugins() {
    return delegate.getBrokerConnectionPlugins();
  }

  @Override
  public List<ActiveMQServerSessionPlugin> getBrokerSessionPlugins() {
    return delegate.getBrokerSessionPlugins();
  }

  @Override
  public List<ActiveMQServerConsumerPlugin> getBrokerConsumerPlugins() {
    return delegate.getBrokerConsumerPlugins();
  }

  @Override
  public List<ActiveMQServerAddressPlugin> getBrokerAddressPlugins() {
    return delegate.getBrokerAddressPlugins();
  }

  @Override
  public List<ActiveMQServerQueuePlugin> getBrokerQueuePlugins() {
    return delegate.getBrokerQueuePlugins();
  }

  @Override
  public List<ActiveMQServerBindingPlugin> getBrokerBindingPlugins() {
    return delegate.getBrokerBindingPlugins();
  }

  @Override
  public List<ActiveMQServerMessagePlugin> getBrokerMessagePlugins() {
    return delegate.getBrokerMessagePlugins();
  }

  @Override
  public List<ActiveMQServerBridgePlugin> getBrokerBridgePlugins() {
    return delegate.getBrokerBridgePlugins();
  }

  @Override
  public List<ActiveMQServerCriticalPlugin> getBrokerCriticalPlugins() {
    return delegate.getBrokerCriticalPlugins();
  }

  @Override
  public List<ActiveMQServerFederationPlugin> getBrokerFederationPlugins() {
    return delegate.getBrokerFederationPlugins();
  }

  @Override
  public List<FederationConfiguration> getFederationConfigurations() {
    return delegate.getFederationConfigurations();
  }
}
