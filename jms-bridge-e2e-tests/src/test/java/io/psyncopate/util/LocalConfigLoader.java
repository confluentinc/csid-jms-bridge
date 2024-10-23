package io.psyncopate.util;

import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@FieldDefaults(makeFinal = true)
public class LocalConfigLoader extends ConfigLoader {
    @Getter
    private String startCommand;
    @Getter
    private String stopCommand;
    @Getter
    private String configFile;
    @Getter
    private String brokerXMLFile;
    @Getter
    private String serverMasterExecutionPath;
    @Getter
    private String serverMasterName;

    @Getter
    private String serverSlaveExecutionPath;
    @Getter
    private String serverSlaveName;

    @Getter
    private String queueName;
    @Getter
    private String topicName;
    @Getter
    private String kafkaTopicName;
    @Getter
    private String kafkaHost;
    @Getter
    private  String kafkaPort;


    @Getter
    private String localConfigPath;
    @Getter
    private String remoteConfigPath;

    private boolean isJaasEnabled;
    @Getter
    private String username;
    @Getter
    private String password;

    private boolean isSslEnabled;
    @Getter
    private String trustStorePath;
    @Getter
    private String trustStoreCertificateName;
    @Getter
    private String trustStorePassword;

    public LocalConfigLoader() {
        super("application.local.properties");

        this.startCommand = properties.getProperty("startCommand");
        this.stopCommand = properties.getProperty("stopCommand");
        this.configFile = properties.getProperty("configFile");

        this.serverMasterExecutionPath = properties.getProperty("server.master.executionPath");
        this.serverMasterName = properties.getProperty("server.master.name");

        this.serverSlaveExecutionPath = properties.getProperty("server.slave.executionPath");
        this.serverSlaveName = properties.getProperty("server.slave.name");

        this.queueName = properties.getProperty("queueName");
        this.topicName = properties.getProperty("topicName");
        this.kafkaTopicName = properties.getProperty("kafkaTopicName");
        this.kafkaHost = properties.getProperty("kafka.host");
        this.kafkaPort = properties.getProperty("kafka.port");
	    this.localConfigPath=properties.getProperty("local.config.file");
        this.remoteConfigPath=properties.getProperty("Remote.config.file");

        this.isJaasEnabled = Boolean.parseBoolean(properties.getProperty("jaas.enabled"));
        this.username=properties.getProperty("jaas.userName");
        this.password=properties.getProperty("jaas.password");

        this.isSslEnabled = Boolean.parseBoolean(properties.getProperty("ssl.enabled"));
        this.trustStorePath=properties.getProperty("ssl.trustStorePath");
        this.trustStoreCertificateName=properties.getProperty("ssl.trustStoreCertificateName");
        this.trustStorePassword=properties.getProperty("ssl.trustStorePassword");
        this.brokerXMLFile=properties.getProperty("brokerXMLFile");
    }

    public boolean isJaasEnabled() {
		return isJaasEnabled;
	}

	public boolean isSslEnabled() {
		return isSslEnabled;
	}
}
