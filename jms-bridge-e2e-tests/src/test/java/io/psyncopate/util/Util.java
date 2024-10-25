package io.psyncopate.util;

import io.psyncopate.GlobalSetup;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.psyncopate.util.constants.BrokerType;
import io.psyncopate.util.constants.Constants;
import io.psyncopate.util.constants.MessagingScheme;
import io.psyncopate.util.constants.RoutingType;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class Util {
    private static final Logger logger = LogManager.getLogger(Util.class);

    private static Path logDirectory;

    public static StringBuilder steps = new StringBuilder();
    public static boolean isDownloadLog = false;
    //public static boolean isMasterServerStarted = false;


    public static String getLogDirectoryPath() {
        return getLogDirectory().toAbsolutePath().toString();
    }
    public static Path getLogDirectory() {
        return logDirectory != null ? logDirectory : createLogDirectory();
    }
    public static Path createLogDirectory() {
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        logDirectory = Paths.get("src/test/logs", "test_run_" + timestamp);
        return createDirectoryIfNotExists(logDirectory);
    }
    public static Path createSubDirectoryInLogIfNotExists(String subDirectoryName) {
        Path subDirectory = getLogDirectory().resolve(subDirectoryName);
        return createDirectoryIfNotExists(subDirectory);
    }
    public static Path createDirectoryIfNotExists(Path path) {
        if (!Files.exists(path)) {
            try {
                Files.createDirectories(path);
                logger.info("Created directory: {}", path.toAbsolutePath());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return path;
    }

    public static String getTestcaseFilePath(String testcaseName, boolean isMaster) {
        String serverType = isMaster ? "Master" : "Slave";
        createSubDirectoryInLogIfNotExists(testcaseName);
        Path testcaseDirectoryPath = createSubDirectoryInLogIfNotExists(testcaseName);
        String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
        return testcaseDirectoryPath.toAbsolutePath() + "/" + testcaseName + "_" + serverType + "_" + timestamp + ".log";
    }

    public static String getCurrentMethodNameByLevel(int traceLevel) {
        return Thread.currentThread().getStackTrace()[traceLevel].getMethodName();
    }

    public static String getMethodNameAsKafkaTopic(int traceLevel) {
        return GlobalSetup.getConfigLoader().getKafkaTopicName()+"-"+Thread.currentThread().getStackTrace()[traceLevel].getMethodName();
    }

    public static String getCurrentMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public static String getMethodNameAsAnycastAddress(int traceLevel) {
        return GlobalSetup.getConfigLoader().getQueueName()+"-"+Thread.currentThread().getStackTrace()[traceLevel].getMethodName();
    }
    public static String getCurrentMethodNameAsAnycastAddress() {
        return getMethodNameAsAnycastAddress(3);
    }

    public static String getCurrentMethodNameAsKafkaTopic() {
        return getMethodNameAsKafkaTopic(3);
    }

    public static String getParentMethodNameAsAnycastAddress() {
        return getMethodNameAsAnycastAddress(4);
    }
    public static String getParentMethodNameAsAddress(MessagingScheme messagingScheme) {
        if(BrokerType.JMS == messagingScheme.getBrokerType()) {
            if(RoutingType.ANYCAST == messagingScheme.getRoutingType()) {
                return getMethodNameAsAnycastAddress(4);
            } else {
                return getMethodNameAsMulticastAddress(4);
            }
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return getMethodNameAsKafkaTopic(3);
        } else {
            return getMethodNameAsMulticastAddress(4);
        }

    }
    public static String getMethodNameAsMulticastAddress(int traceLevel) {
        return GlobalSetup.getConfigLoader().getTopicName()+"-"+Thread.currentThread().getStackTrace()[traceLevel].getMethodName();
    }

    public static boolean downloadLog(String testcaseName, boolean isMaster) {

        return GlobalSetup.getServerSetup().downloadLog( testcaseName, isMaster);
    }

    public static boolean isPortEstablished(String command, String searchStr) {

        try {
            Process process = Runtime.getRuntime().exec(command);
            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains(searchStr)) {
                    return true;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    public static String checkSeverStatus(String jmxHost, int jmxPort) {
        // JMX connection parameters

        String jmxURL = "service:jmx:rmi:///jndi/rmi://" + jmxHost + ":" + jmxPort + "/jmxrmi";
        String brokerObjectName = "org.apache.activemq.artemis:broker=\"localhost\""; // Replace with your broker's ObjectName

        try {
            // Connect to the JMX server
            JMXServiceURL serviceURL = new JMXServiceURL(jmxURL);
            JMXConnector jmxConnector = JMXConnectorFactory.connect(serviceURL, null);
            MBeanServerConnection mbsc = jmxConnector.getMBeanServerConnection();

            // Query the 'Backup' attribute of the broker MBean
            ObjectName objectName = new ObjectName(brokerObjectName);
            Boolean isBackup = (Boolean) mbsc.getAttribute(objectName, "Backup");

            // Check the status and print the result
            if (isBackup != null && isBackup) {
                logger.info("The {} is in standby mode.", jmxHost);
                return Constants.STANDBY;
                //System.out.println("The broker is in standby mode.");
            } else {
                //System.out.println("The broker is active.");
                logger.info("The {} is active.", jmxHost);
                return Constants.ACTIVE;
            }

            // Close the JMX connection

        } catch (Exception e) {
            return "Failed";
        }
    }

    public static void addSteps(StringBuilder steps, String message) {
        steps.append(message).append("\n");
    }

    public static void getKafkaTopicInfo(String bootstrapServers, String topicName) throws ExecutionException, InterruptedException {
        Properties config = new Properties();
        config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(config)) {


            DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
            Map<String, TopicDescription> topicDescriptions = describeTopicsResult.all().get();
            Map<String, Object> topicInfoMap = new HashMap<>();
            topicInfoMap.put("topicDescription", topicDescriptions.get(topicName));

            ConfigResource topicResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
            DescribeConfigsResult configsResult = adminClient.describeConfigs(Collections.singletonList(topicResource));
            Config topicConfig = configsResult.all().get().get(topicResource);

            for (ConfigEntry configEntry : topicConfig.entries()) {
                topicInfoMap.put(configEntry.name(), configEntry.value());
            }

            long totalMessageCount = getMessageCount(topicName, bootstrapServers);
            topicInfoMap.put("messageCount", totalMessageCount);
            System.out.println("Topic Information:");
            topicInfoMap.forEach((key, value) -> System.out.println(key + ": " + value));
        }
    }

    private static long getMessageCount(String topicName, String bootstrapServers) {
        Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-topic-info-consumer");
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig)) {
            List<TopicPartition> partitions = new ArrayList<>();
            consumer.partitionsFor(topicName).forEach(partitionInfo -> {
                partitions.add(new TopicPartition(topicName, partitionInfo.partition()));
            });
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);
            return endOffsets.values().stream().mapToLong(Long::longValue).sum();
        }
    }

    public static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
