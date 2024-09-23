package io.confluent.jms.bridge.server;

import io.confluent.jms.bridge.client.JMSClient;
import lombok.Builder;
import lombok.Data;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static io.confluent.jms.bridge.util.Util.steps;

@Data
@Builder
public class ServerConfig {
    private static String SCRIPT_BASE_PATH = "src/test/resources/scripts/";
    private String name;
    private String port;
    private String path;
    private String host;

    public static class ServerControl {

        private static final Logger logger = LogManager.getLogger(ServerControl.class);

        public boolean resetKafkaAndLocalState() {
            String scriptPath = SCRIPT_BASE_PATH + "reset.sh";
            return executeScript(getAbsolutePath(scriptPath));
        }

        public boolean startServer(io.confluent.jms.bridge.util.ConfigLoader configLoader, boolean isMaster) {
            String scriptPath = SCRIPT_BASE_PATH + "server-start.sh";
            return executeScript(getArgsByNode(scriptPath, configLoader, isMaster, true, false));
        }

        private static List<String> getArgsByNode(String scriptPath, io.confluent.jms.bridge.util.ConfigLoader configLoader, boolean isMaster, boolean isStartCmd, boolean killServer) {
            String scriptAbsolutePath = getAbsolutePath(scriptPath);
            String serverConfigToUse = isMaster ? io.confluent.jms.bridge.util.constants.Constants.SERVER_MASTER : io.confluent.jms.bridge.util.constants.Constants.SERVER_SLAVE;
            String name = configLoader.getServersConfig().get(serverConfigToUse).getName();
            String port = configLoader.getServersConfig().get(serverConfigToUse).getPort();
            String path = configLoader.getServersConfig().get(serverConfigToUse).getPath();
            String command = isStartCmd ? configLoader.getStartCommand() : configLoader.getStopCommand();
            String mode = isMaster ? io.confluent.jms.bridge.util.constants.Constants.ACTIVE : io.confluent.jms.bridge.util.constants.Constants.STANDBY;
            String logFilePath = io.confluent.jms.bridge.util.Util.getLogDirectoryPath() + "/server_command.log";
            String kill = killServer ? "Kill" : "";
            if (isStartCmd) {
                logger.info("Executing command: {}", concatenateStrings(" ", scriptAbsolutePath, name, port, path, command, mode, logFilePath));
                return Arrays.asList(scriptAbsolutePath, name, port, path, command, mode, logFilePath);
            } else {
                logger.info("Executing command: {}", concatenateStrings(" ", scriptAbsolutePath, name, port, path, command, logFilePath, kill));
                return Arrays.asList(scriptAbsolutePath, name, port, path, command, logFilePath, kill);
            }
        }

        public boolean stopServer(io.confluent.jms.bridge.util.ConfigLoader configLoader, boolean isMaster, boolean killServer) {
            String scriptPath = SCRIPT_BASE_PATH + "server-stop.sh";
            return executeScript(getArgsByNode(scriptPath, configLoader, isMaster, false, killServer));
        }

        public boolean downloadLog(io.confluent.jms.bridge.util.ConfigLoader configLoader, String testcaseName, boolean isMaster) {
            String serverConfigToUse = isMaster ? io.confluent.jms.bridge.util.constants.Constants.SERVER_MASTER : io.confluent.jms.bridge.util.constants.Constants.SERVER_SLAVE;
            String logPath = configLoader.getServersConfig().get(serverConfigToUse).getPath();
            String remoteLogFile = logPath + "/logs/jms-bridge.out";
            String localFilePath = io.confluent.jms.bridge.util.Util.getTestcaseFilePath(testcaseName, isMaster); // Store log with test name
            List<String> command = Arrays.asList("cp", remoteLogFile, localFilePath);
            return executeScript(command);
        }


        static String getAbsolutePath(String path) {
            File file = new File(path);
            return file.getAbsolutePath();
        }

        public boolean executeScript(String command) {
            //String result = executeCommand(command);
            //return ServerStatus.valueOf(result.toUpperCase());
            return executeCommand(command);
        }

        public static String concatenateStrings(String delimiter, String... strings) {
            StringBuilder result = new StringBuilder();
            for (int i = 0; i < strings.length; i++) {
                result.append(strings[i]);
                if (i < strings.length - 1) {
                    result.append(delimiter);
                }
            }
            return result.toString();
        }

        /*public boolean isPortEstablished(HashMap<String, String> server, String searchStr) {
            ConfigLoader configLoader = new ConfigLoader();

            String command = configLoader.getPemPath() != null ? "ssh -i " + configLoader.getPemPath() + " ec2-user@" + server.get(Constants.HOST) + "lsof -i:" + server.get(Constants.APP_PORT) :
                    "sshpass -p xcadmin ssh " + server.get(Constants.HOST) + " lsof -i:" + server.get(Constants.APP_PORT);

            return executeEstablishedOrActive(command, searchStr);
        }*/

       /* public static boolean isPortOpen(ConfigLoader configLoader, boolean isMaster) {
            String scriptPath = "src/test/java/org/example/script/server-up.sh";
            return executeScript(getArgsByNode(scriptPath, configLoader, isMaster, true, false));
        }*/
        //    private boolean executeEstablishedOrActive(String command,String searchStr) {
        //
        //        try {
        //            Process process = Runtime.getRuntime().exec(command);
        //            BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
        //            String line;
        //            while ((line = reader.readLine()) != null) {
        //                if (line.contains(searchStr)) {
        //                    return true;
        //                }
        //            }
        //        } catch (IOException e) {
        //            e.printStackTrace();
        //        }
        //        return false;
        //    }

        private boolean executeCommand(String command) {
            Process process = null;
            String status = "";
            logger.debug("Command to execute: {}", command);
            try {
                process = Runtime.getRuntime().exec(command);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    status = line;
                    System.out.println(line);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            // Wait for the process to finish and get the exit code
            int exitCode = 1;
            try {
                exitCode = process.waitFor();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            // Check if the script was successful
            if (exitCode == 0) {
                logger.debug("Script executed successfully with exit code: {}", exitCode);
                return true;
            } else {
                System.err.println("Script execution failed with exit code: " + exitCode);
                throw new RuntimeException("Script failed with exit code " + exitCode);
            }
        }

        private static boolean executeScript(List<String> command) {
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.redirectErrorStream(true);
            Process process = null;
            try {
                process = processBuilder.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    System.out.println(line);
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            int exitCode = 0;
            try {
                exitCode = process.waitFor();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            if (exitCode == 0) {
                System.out.println("Script executed successfully.");
            } else {
                System.err.println("Script executed failed to execute. Exit code: " + exitCode);
            }
            return exitCode == 0;
        }
    }

    public static class ServerSetup {
        private static final Logger logger = LogManager.getLogger(ServerSetup.class);

        //    static {
        //        String logFilePath = Util.getLogDirectoryPath() + "/testcases_" + new SimpleDateFormat("yyyyMMdd_HHmmss").format(new Date()) + ".log";
        //        System.setProperty("logFilePath", logFilePath);
        //    }

        public static boolean startMasterServer() {

            ServerControl serverControl = new ServerControl();
            boolean isSuccess = serverControl.startServer(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, true);
            if (isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Started Master Server");
            }
            return isSuccess;
        }

        public static boolean startSlaveServer() {
            ServerControl serverControl = new ServerControl();
            boolean isSuccess = serverControl.startServer(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, false);
            if (isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Started Slave Server");
            }
            return isSuccess;
        }

        public static boolean stopMasterServer() {
            ServerControl serverControl = new ServerControl();
            boolean isSuccess = serverControl.stopServer(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, true, false);
            if (isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Stopped Master Server");
                if (io.confluent.jms.bridge.util.Util.isDownloadLog) {
                    serverControl.downloadLog(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, io.confluent.jms.bridge.util.Util.getCurrentMethodNameByLevel(3), true);
                }
            }
            return isSuccess;
        }


        public static boolean stopSlaveServer() {
            ServerControl serverControl = new ServerControl();
            boolean isSuccess = serverControl.stopServer(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, false, false);
            if (isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Stopped Slave Server");
                if (io.confluent.jms.bridge.util.Util.isDownloadLog) {
                    serverControl.downloadLog(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, io.confluent.jms.bridge.util.Util.getCurrentMethodNameByLevel(3), false);
                }
            }
            return isSuccess;
        }

        public static boolean killSlaveServer() {
            ServerControl serverControl = new ServerControl();
            boolean isSuccess = serverControl.stopServer(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, false, true);
            if (isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Killed Master Server");
            }
            return isSuccess;
        }

        public static boolean killSlaveServer(int delayInSecs) {
            try {
                Thread.sleep(delayInSecs * 1000L);
            } catch (InterruptedException e) {
                logger.info("Delay was interrupted while stopping server", e);
            }
            return killSlaveServer();
        }

        public static boolean killMasterServer() {
            ServerControl serverControl = new ServerControl();
            boolean isSuccess = serverControl.stopServer(io.confluent.jms.bridge.util.Util.CONFIG_LOADER, true, true);
            if (isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Killed Master Server");
            }
            return isSuccess;
        }

        public static boolean killMasterServer(int delayInSecs) {
            try {
                Thread.sleep(delayInSecs * 1000L);
            } catch (InterruptedException e) {
                logger.info("Delay was interrupted while stopping server", e);
            }
            return killMasterServer();
        }

        public static int startJmsProducer(HashMap<String, String> currentNode, String destination, int totalMessage, io.confluent.jms.bridge.util.constants.AddressScheme addressScheme) {
            io.confluent.jms.bridge.util.Util.addSteps(steps, "Started JMS Producer");
            JMSClient jmsClient = new JMSClient();
            //TODO: addressScheme is unused - need to implement or remove it... - think in general ANYCAST is when Queue is created and MULTICAST when Topic is created
            int sentMessagesCount = jmsClient.produceMessages(currentNode, totalMessage, destination);
            io.confluent.jms.bridge.util.Util.addSteps(steps, "Stopped JMS Producer");
            return sentMessagesCount;
        }

        public static int startJmsProducer(String destination, int totalMessage, io.confluent.jms.bridge.util.constants.AddressScheme addressScheme) {
            return startJmsProducer(null, destination, totalMessage, addressScheme);
        }

        public static CompletableFuture<Integer> startJmsProducerAsync(String destination, int messageCountToBeSent, io.confluent.jms.bridge.util.constants.AddressScheme addressScheme) {
            return CompletableFuture.supplyAsync(() -> startJmsProducer(destination, messageCountToBeSent, addressScheme));
        }

        public static CompletableFuture<Integer> startJmsProducerAsync(String destination, io.confluent.jms.bridge.util.constants.AddressScheme addressScheme) {
            return startJmsProducerAsync(destination, -1, addressScheme);
        }

        public static int startJmsConsumer(HashMap<String, String> currentNode, String destination, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
            JMSClient jmsClient = new JMSClient();
            if (messageCount == -1) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Started JMS Consumer to consume all messages");
            } else {
                io.confluent.jms.bridge.util.Util.addSteps(steps, "Started JMS Consumer to consume " + messageCount + " messages");
            }
            int consumedMessagesCount = jmsClient.consumeMessages(currentNode, destination, messageCount, sleepInMillis);
            io.confluent.jms.bridge.util.Util.addSteps(steps, "Stopped JMS Consumer");
            return consumedMessagesCount;
        }

        public static int startJmsConsumer(String destination, int messageCount, Long sleepInMillis) throws JMSException, InterruptedException {
            return startJmsConsumer(null, destination, messageCount, sleepInMillis);
        }

        public static CompletableFuture<Integer> startJmsConsumerAsync(String destination, int totalMessage, Long sleepInMillis) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return startJmsConsumer(destination, totalMessage, sleepInMillis);
                } catch (JMSException | InterruptedException e) {
                    logger.error("Failed to send messages asynchronously: {}", e.getMessage());
                    return 0;
                }
            });
        }


        //    public static CompletableFuture<Integer> startJmsConsumerAsync(HashMap<String, String> currentNode, String destination, int totalMessage) {
        //        return CompletableFuture.supplyAsync(() -> {
        //            try {
        //                return startJmsConsumer(currentNode, destination, totalMessage);
        //            } catch (JMSException e) {
        //                logger.error("Failed to send messages asynchronously: {}", e.getMessage());
        //                return 0;
        //            }
        //        });
        //    }

        public static CompletableFuture<Integer> startJmsConsumerAsync(String destination, Long sleepInMillis) {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    return startJmsConsumer(destination, sleepInMillis);
                } catch (JMSException | InterruptedException e) {
                    logger.error("Failed to consume messages asynchronously: {}", e.getMessage());
                    return 0;
                }
            });
        }

        public static int startJmsConsumer(String destination, Long sleepInMillis) throws JMSException, InterruptedException {
            return startJmsConsumer(destination, -1, sleepInMillis);
        }

        public static int startJmsConsumer(String destination) throws JMSException, InterruptedException {
            return startJmsConsumer(destination, -1, 0L);
        }

        /**
         * Check if the server is up
         *
         * @param serverType             The type of server to check
         * @param retryIntervalInSeconds The interval in seconds to wait before retrying
         * @param maxRetries             The maximum number of retries
         * @return true if the server is up, false otherwise
         */
        public static boolean isServerUp(io.confluent.jms.bridge.util.constants.ServerType serverType, int retryIntervalInSeconds, int maxRetries) {
            maxRetries = maxRetries < 1 ? 1 : 3;
            HashMap<String, String> currentNode = serverType.equals(io.confluent.jms.bridge.util.constants.ServerType.MASTER) ? io.confluent.jms.bridge.util.Util.getMasterServer() : io.confluent.jms.bridge.util.Util.getSlaveServer();
            boolean isSuccess = false;

            for (int i = 0; i < maxRetries; i++) {
                isSuccess = io.confluent.jms.bridge.util.Util.isPortOpen(currentNode);
                if (isSuccess) {
                    io.confluent.jms.bridge.util.Util.addSteps(steps, serverType.toString() + " Server now live");
                    break;
                } else {
                    logger.info("Retry {}/{}: {} Server is not up. Waiting for {} seconds before retrying...", i + 1, maxRetries, serverType, retryIntervalInSeconds);
                    try {
                        Thread.sleep(retryIntervalInSeconds * 1000L);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        logger.error("Thread was interrupted", e);
                        break;
                    }
                }
            }

            if (!isSuccess) {
                io.confluent.jms.bridge.util.Util.addSteps(steps, serverType.toString() + " Server did not start within the expected time.");
            }

            return isSuccess;
        }

        public static void resetKafkaAndLocalState() {
            new ServerControl().resetKafkaAndLocalState();
        }
    }
}
