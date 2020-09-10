/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static java.lang.String.format;

import io.confluent.amq.config.BridgeConfig;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.LegacyJMSConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfluentEmbeddedAmqImpl implements ConfluentEmbeddedAmq {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfluentEmbeddedAmqImpl.class);

  private InternalEmbedded embeddedActiveMQ;

  protected ConfluentEmbeddedAmqImpl(JmsBridgeConfiguration configuration,
      MBeanServer mbeanServer, ActiveMQSecurityManager securityManager) {

    this.embeddedActiveMQ = new InternalEmbedded();
    this.embeddedActiveMQ.setSecurityManager(securityManager)
        .setMbeanServer(mbeanServer)
        .setConfiguration(configuration);

    this.embeddedActiveMQ.prepare();
  }

  public boolean waitClusterForming(long timeWait, TimeUnit unit, int iterations, int servers)
      throws Exception {
    return embeddedActiveMQ.waitClusterForming(timeWait, unit, iterations, servers);
  }

  public ActiveMQServer getAmq() {
    return embeddedActiveMQ.getActiveMQServer();
  }

  public void start() throws Exception {
    embeddedActiveMQ.start();
  }

  public void stop() throws Exception {
    embeddedActiveMQ.stop();
  }

  public static class Builder {

    private ActiveMQSecurityManager securityManager;
    private JmsBridgeConfiguration configuration;
    private MBeanServer mbeanServer;

    public Builder(BridgeConfig bridgeConfig) {
      this("/broker.xml", bridgeConfig);
    }

    public Builder(String configResourcePath, BridgeConfig bridgeConfig) {
      this.configuration = loadConfigResource(configResourcePath, bridgeConfig);
    }

    public Builder(JmsBridgeConfiguration configuration) {
      this.configuration = configuration;
    }

    private JmsBridgeConfiguration loadConfigResource(
        String resourcePath, BridgeConfig bridgeConfig) {

      FileDeploymentManager deploymentManager = new FileDeploymentManager(resourcePath);
      FileConfiguration config = new FileConfiguration();
      LegacyJMSConfiguration legacyJmsConfiguration = new LegacyJMSConfiguration(config);
      deploymentManager.addDeployable(config).addDeployable(legacyJmsConfiguration);
      try {
        deploymentManager.readConfiguration();
      } catch (Exception e) {
        throw new RuntimeException(
            format("Failed to read configuration resource '%s'", resourcePath), e);
      }
      return new JmsBridgeConfiguration(config, bridgeConfig);
    }

    public Builder setSecurityManager(ActiveMQSecurityManager securityManager) {
      this.securityManager = securityManager;
      return this;
    }


    public Builder setMbeanServer(MBeanServer mbeanServer) {
      this.mbeanServer = mbeanServer;
      return this;
    }

    public ConfluentEmbeddedAmqImpl build() {
      return new ConfluentEmbeddedAmqImpl(this.configuration, this.mbeanServer,
          this.securityManager);
    }
  }

  private static class InternalEmbedded extends EmbeddedActiveMQ {

    protected void prepare() {
      //this.configuration.registerBrokerPlugin(new KafkaBridgePlugin(kafkaProps));
      if (this.securityManager == null) {
        this.securityManager = new ActiveMQJAASSecurityManager();
      }

      DelegatingConfluentAmqServer amqServer = null;
      JmsBridgeConfiguration jmsBridgeConfiguration = (JmsBridgeConfiguration) configuration;

      if (this.mbeanServer == null) {
        amqServer = new DelegatingConfluentAmqServer(jmsBridgeConfiguration, this.securityManager);
      } else {
        amqServer = new DelegatingConfluentAmqServer(
            jmsBridgeConfiguration, this.mbeanServer, this.securityManager);
      }
      this.activeMQServer = amqServer;
      this.activeMQServer.registerActivationFailureListener((e) -> {
        LOGGER.error("Shutting down JMS-Bridge due to unrecoverable failures.");
        try {
          this.activeMQServer.stop(true, true);
        } catch (Exception ex) {
          LOGGER.error("Failed to gracefully shutdown JMS-Bridge", ex);
        }
      });
    }

    @Override
    protected void initStart() throws Exception {
      //do nothing, see prepare
    }
  }
}
