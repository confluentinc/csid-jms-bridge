package io.psyncopate.util;

import lombok.Getter;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class ConfigLoader {
    @Getter
    private final String startCommand;
    @Getter
    private final String stopCommand;
    @Getter
    private final String masterConfigFile;
    @Getter
    private final String slaveConfigFile;
    @Getter
    private final String remoteConfigFile;
    @Getter
    private final String brokerXMLFile;
    @Getter
    private final String queueName;
    @Getter
    private final String topicName;
    @Getter
    private final String kafkaTopicName;
    @Getter
    private final String kafkaHost;
    @Getter
    private final String kafkaPort;


    @Getter
    private final String localConfigPath;
    @Getter
    private final String remoteConfigPath;

    private final boolean isJaasEnabled;
    @Getter
    private final String username;
    @Getter
    private final String password;

    private final boolean isSslEnabled;
    @Getter
    private final String trustStorePath;
    @Getter
    private final String trustStoreCertificateName;
    @Getter
    private final String trustStorePassword;

    @Getter
    private final String serverMasterHost;
    @Getter
    private final String serverMasterAppPort;

    @Getter
    private final String serverSlaveHost;
    @Getter
    private final String serverSlaveAppPort;


    @Getter
    protected final Properties properties = new Properties();


    public ConfigLoader(String propsFile) {
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream(propsFile)) {
            properties.load(input);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties file " + propsFile);
        }

        this.startCommand = properties.getProperty("startCommand");
        this.stopCommand = properties.getProperty("stopCommand");
        this.masterConfigFile = properties.getProperty("master.configFile");
        this.slaveConfigFile = properties.getProperty("slave.configFile");
        this.remoteConfigFile = properties.getProperty("remote.configFile");

        this.queueName = properties.getProperty("queueName");
        this.topicName = properties.getProperty("topicName");
        this.kafkaTopicName = properties.getProperty("kafkaTopicName");
        this.kafkaHost = properties.getProperty("kafka.host");
        this.kafkaPort = properties.getProperty("kafka.port");
        this.localConfigPath = properties.getProperty("local.config.file");
        this.remoteConfigPath = properties.getProperty("Remote.config.file");

        this.isJaasEnabled = Boolean.parseBoolean(properties.getProperty("jaas.enabled"));
        this.username = properties.getProperty("jaas.userName");
        this.password = properties.getProperty("jaas.password");

        this.isSslEnabled = Boolean.parseBoolean(properties.getProperty("ssl.enabled"));
        this.trustStorePath = properties.getProperty("ssl.trustStorePath");
        this.trustStoreCertificateName = properties.getProperty("ssl.trustStoreCertificateName");
        this.trustStorePassword = properties.getProperty("ssl.trustStorePassword");
        this.brokerXMLFile = properties.getProperty("brokerXMLFile");
        this.serverMasterAppPort = properties.getProperty("server.master.appPort");
        this.serverMasterHost = properties.getProperty("server.master.host");
        this.serverSlaveAppPort = properties.getProperty("server.slave.appPort");
        this.serverSlaveHost = properties.getProperty("server.slave.host");
    }


    public boolean isJaasEnabled() {
        return isJaasEnabled;
    }

    public boolean isSslEnabled() {
        return isSslEnabled;
    }
}
