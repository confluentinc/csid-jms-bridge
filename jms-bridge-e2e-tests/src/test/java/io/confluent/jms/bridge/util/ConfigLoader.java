package io.confluent.jms.bridge.util;

import lombok.Getter;
import lombok.experimental.FieldDefaults;
import io.confluent.jms.bridge.server.ServerConfig;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@Getter
@FieldDefaults(makeFinal = true, level = lombok.AccessLevel.PRIVATE)
public class ConfigLoader {
    private String startCommand;
    private String stopCommand;

    private Map<String, ServerConfig> serversConfig;
    private String queueName;
    private String topicName;
    private Properties properties;

    public ConfigLoader() {
        this.properties = new Properties();
        try (InputStream input = ConfigLoader.class.getClassLoader().getResourceAsStream("application.properties")) {
            this.properties.load(input);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load properties file");
        }
        this.startCommand = properties.getProperty("startCommand");
        this.stopCommand = properties.getProperty("stopCommand");

        this.queueName = properties.getProperty("queueName");
        this.topicName = properties.getProperty("topicName");
        this.serversConfig = parseServersConfig(properties);
    }

    private Map<String, ServerConfig> parseServersConfig(Properties properties) {
        Map<String, ServerConfig> serversConfig = new HashMap<>();
        Set<String> serverNames = new HashSet<>();

        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("server.")) {
                String[] parts = key.split("\\.");
                String serverConfigKey = parts[1];
                serverNames.add(serverConfigKey);
            }
        }
        for (String serverConfigKey : serverNames) {
            String serverPort = properties.getProperty("server." + serverConfigKey + ".port");
            String serverName = properties.getProperty("server." + serverConfigKey + ".name");
            String serverPath = properties.getProperty("server." + serverConfigKey + ".path");
            String serverHost = properties.getProperty("server." + serverConfigKey + ".host");
            serversConfig.put(serverConfigKey, ServerConfig.builder().name(serverName).port(serverPort).path(serverPath).host(serverHost).build());
        }
        return serversConfig;
    }
}
