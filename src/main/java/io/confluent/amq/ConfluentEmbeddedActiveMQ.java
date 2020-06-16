package io.confluent.amq;

import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.FileDeploymentManager;
import org.apache.activemq.artemis.core.config.impl.FileConfiguration;
import org.apache.activemq.artemis.core.config.impl.LegacyJMSConfiguration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.apache.activemq.artemis.spi.core.security.ActiveMQJAASSecurityManager;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;
import org.jboss.logging.Logger;

import javax.management.MBeanServer;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;

public class ConfluentEmbeddedActiveMQ {
    private static final Logger logger = Logger.getLogger(ConfluentEmbeddedActiveMQ.class);

    private InternalEmbedded embeddedActiveMQ;
    private Properties kafkaProps;

    protected ConfluentEmbeddedActiveMQ(Configuration configuration, Properties kafkaProps, MBeanServer mBeanServer, ActiveMQSecurityManager securityManager) {
        this.kafkaProps = kafkaProps;
        this.embeddedActiveMQ = new InternalEmbedded();
        this.embeddedActiveMQ.setSecurityManager(securityManager)
                             .setMbeanServer(mBeanServer)
                             .setConfiguration(configuration);

        this.embeddedActiveMQ.prepare();
    }

    public boolean waitClusterForming(long timeWait, TimeUnit unit, int iterations, int servers) throws Exception {
        return embeddedActiveMQ.waitClusterForming(timeWait, unit, iterations, servers);
    }

    public ActiveMQServer getAmq() {
        return embeddedActiveMQ.getActiveMQServer();
    }

    public ConfluentEmbeddedActiveMQ start() throws Exception {
        embeddedActiveMQ.start();
        return this;
    }

    public ConfluentEmbeddedActiveMQ stop() throws Exception {
        embeddedActiveMQ.stop();
        return this;
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

    public static class Builder {
        private ActiveMQSecurityManager securityManager;
        private String configResourcePath = null;
        private Configuration configuration;
        private ActiveMQServer activeMQServer;
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
            LegacyJMSConfiguration legacyJMSConfiguration = new LegacyJMSConfiguration(config);
            deploymentManager.addDeployable(config).addDeployable(legacyJMSConfiguration);
            try {
                deploymentManager.readConfiguration();
            } catch (Exception e) {
                throw new RuntimeException(format("Failed to read configuration resource '%s'", resourcePath));
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

        public ConfluentEmbeddedActiveMQ build() {
            return new ConfluentEmbeddedActiveMQ(this.configuration, this.kafkaProps, this.mbeanServer, this.securityManager);
        }
    }
}
