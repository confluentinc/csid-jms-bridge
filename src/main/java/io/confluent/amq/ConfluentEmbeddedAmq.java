/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static java.lang.String.format;

import java.util.Properties;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.LegacyJMSConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.jboss.logging.Logger;

public class ConfluentEmbeddedAmq {

  private static final Logger logger = Logger.getLogger(ConfluentEmbeddedAmq.class);

  private InternalEmbedded embeddedActiveMQ;
  private Properties kafkaProps;

  protected ConfluentEmbeddedAmq(Configuration configuration, Properties kafkaProps,
      MBeanServer mbeanServer, ActiveMQSecurityManager securityManager) {
    this.kafkaProps = kafkaProps;
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

  public ConfluentEmbeddedAmq start() throws Exception {
    embeddedActiveMQ.start();
    return this;
  }

  public ConfluentEmbeddedAmq stop() throws Exception {
    embeddedActiveMQ.stop();
    return this;
  }

  public static class Builder {

    private ActiveMQSecurityManager securityManager;
    private Configuration configuration;
    private MBeanServer mbeanServer;
    private Properties kafkaProps;

    public Builder(Properties kafkaProps) {
      this("broker.xml", kafkaProps);
    }

    public Builder(String configResourcePath, Properties kafkaProps) {
      this.configuration = loadConfigResource(configResourcePath);
      this.kafkaProps = kafkaProps;
    }

    public Builder(Configuration configuration, Properties kafkaProps) {
      this.configuration = configuration;
      this.kafkaProps = kafkaProps;
    }

    private Configuration loadConfigResource(String resourcePath) {
      FileDeploymentManager deploymentManager = new FileDeploymentManager(resourcePath);
      FileConfiguration config = new FileConfiguration();
      LegacyJMSConfiguration legacyJmsConfiguration = new LegacyJMSConfiguration(config);
      deploymentManager.addDeployable(config).addDeployable(legacyJmsConfiguration);
      try {
        deploymentManager.readConfiguration();
      } catch (Exception e) {
        throw new RuntimeException(
            format("Failed to read configuration resource '%s'", resourcePath));
      }
      return config;
    }

    public Builder setSecurityManager(ActiveMQSecurityManager securityManager) {
      this.securityManager = securityManager;
      return this;
    }


    public Builder setMbeanServer(MBeanServer mbeanServer) {
      this.mbeanServer = mbeanServer;
      return this;
    }

    public ConfluentEmbeddedAmq build() {
      return new ConfluentEmbeddedAmq(this.configuration, this.kafkaProps, this.mbeanServer,
          this.securityManager);
    }
  }

  private class InternalEmbedded extends EmbeddedActiveMQ {

    protected void prepare() {
      this.configuration.registerBrokerPlugin(new KafkaBridgePlugin(kafkaProps));
      if (this.securityManager == null) {
        this.securityManager = new ActiveMQJAASSecurityManager();
      }

      ConfluentAmqServer amqServer = null;

      if (this.mbeanServer == null) {
        amqServer = new ConfluentAmqServer(configuration, this.securityManager);
      } else {
        amqServer = new ConfluentAmqServer(configuration, this.mbeanServer, this.securityManager);
      }
      amqServer.setKafkaProps(kafkaProps);
      this.activeMQServer = amqServer;
    }

    @Override
    protected void initStart() throws Exception {
      //do nothing, see prepare
    }
  }
}
