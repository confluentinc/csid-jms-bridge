package io.confluent.amq;

import io.confluent.amq.persistence.kafka.KafkaJournalStorageManager;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.ServiceRegistry;
import org.apache.activemq.artemis.core.server.impl.ActiveMQServerImpl;
import org.apache.activemq.artemis.spi.core.security.ActiveMQSecurityManager;

import javax.management.MBeanServer;
import java.util.Properties;

public class ConfluentAmqServer extends ActiveMQServerImpl {
    private Properties kafkaProps;

    public ConfluentAmqServer() {
    }

    public ConfluentAmqServer(Configuration configuration) {
        super(configuration);
    }

    public ConfluentAmqServer(Configuration configuration, ActiveMQServer parentServer) {
        super(configuration, parentServer);
    }

    public ConfluentAmqServer(Configuration configuration, MBeanServer mbeanServer) {
        super(configuration, mbeanServer);
    }

    public ConfluentAmqServer(Configuration configuration, ActiveMQSecurityManager securityManager) {
        super(configuration, securityManager);
    }

    public ConfluentAmqServer(Configuration configuration, MBeanServer mbeanServer, ActiveMQSecurityManager securityManager) {
        super(configuration, mbeanServer, securityManager);
    }

    public ConfluentAmqServer(Configuration configuration, MBeanServer mbeanServer, ActiveMQSecurityManager securityManager, ActiveMQServer parentServer) {
        super(configuration, mbeanServer, securityManager, parentServer);
    }

    public ConfluentAmqServer(Configuration configuration, MBeanServer mbeanServer, ActiveMQSecurityManager securityManager, ActiveMQServer parentServer, ServiceRegistry serviceRegistry) {
        super(configuration, mbeanServer, securityManager, parentServer, serviceRegistry);
    }

    public void setKafkaProps(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;
    }


    @Override
    protected StorageManager createStorageManager() {
        if(this.kafkaProps == null || this.kafkaProps.isEmpty()) {
            throw new IllegalStateException("KafkaProps must be set before starting the server.");
        }

        Configuration configuration = getConfiguration();
        KafkaJournalStorageManager journal = new KafkaJournalStorageManager(configuration, getCriticalAnalyzer(), executorFactory, scheduledPool, ioExecutorFactory, shutdownOnCriticalIO);
        journal.setKafkaProps(kafkaProps);
        this.getCriticalAnalyzer().add(journal);
        return journal;
    }


}
