package io.confluent.jms.bridge.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import io.confluent.jms.bridge.server.ServerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.confluent.jms.bridge.util.constants.Constants;

import javax.management.MBeanServerConnection;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class Util {
    private static final Logger logger = LogManager.getLogger(Util.class);

    private static Path logDirectory;

    public static final ConfigLoader CONFIG_LOADER = new ConfigLoader();

    private static final HashMap<String, String> masterServer = new HashMap<>();

    private static final HashMap<String, String> slaveServer = new HashMap<>();

    public static boolean isDownloadLog = false;
    //public static boolean isMasterServerStarted = false;

    static {
        masterServer.put(Constants.HOST, CONFIG_LOADER.getServersConfig().get(Constants.SERVER_MASTER).getHost());
        masterServer.put(Constants.APP_PORT, CONFIG_LOADER.getServersConfig().get(Constants.SERVER_MASTER).getPort());

        slaveServer.put(Constants.HOST, CONFIG_LOADER.getServersConfig().get(Constants.SERVER_SLAVE).getHost());
        slaveServer.put(Constants.APP_PORT, CONFIG_LOADER.getServersConfig().get(Constants.SERVER_SLAVE).getPort());
    }

    public static HashMap<String, String> getMasterServer() {
        return masterServer;
    }

    public static HashMap<String, String> getSlaveServer() {
        return slaveServer;
    }

    public static StringBuilder steps = new StringBuilder();

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
    public static boolean isPortOpen(HashMap<String, String> currentNode) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(currentNode.get(Constants.HOST), Integer.parseInt(currentNode.get(Constants.APP_PORT))), 2000); // Timeout is 2000 milliseconds
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public static String getCurrentMethodNameByLevel(int traceLevel) {
        return Thread.currentThread().getStackTrace()[traceLevel].getMethodName();
    }

    public static String getCurrentMethodName() {
        return Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public static String getMethodNameAsQueueName() {
        return CONFIG_LOADER.getQueueName()+"-"+Thread.currentThread().getStackTrace()[2].getMethodName();
    }

    public static boolean downloadLog(String testcaseName, boolean isMaster) {
        ServerConfig.ServerControl serverControl = new ServerConfig.ServerControl();
        return serverControl.downloadLog(CONFIG_LOADER, testcaseName, isMaster);
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

    public static void closeQuietly(AutoCloseable objectToClose) {
        try {
            if(objectToClose==null) {
                return;
            }
            objectToClose.close();
        } catch (Exception e) {
            logger.warn("Failed to close object {}: ", objectToClose, e);
        }

    }
}
