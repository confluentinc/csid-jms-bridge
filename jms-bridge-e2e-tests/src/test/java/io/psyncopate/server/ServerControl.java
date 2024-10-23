package io.psyncopate.server;

import io.psyncopate.JMSQueueTest;
import io.psyncopate.JMSTopicTest;
import io.psyncopate.KafkaJMSTopicTest;
import io.psyncopate.util.ConfigLoader;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.ServerType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static io.psyncopate.util.Util.steps;

public abstract class ServerControl {

    private static final Logger logger = LogManager.getLogger(ServerControl.class);

    public abstract boolean startServer(boolean isMaster);

    public abstract boolean stopServer(boolean isMaster, boolean killServer);


    public abstract boolean downloadLog(String testcaseName, boolean isMaster);

    String getAbsolutePath(String path) {
        File file = new File(path);
        return file.getAbsolutePath();
    }


    public boolean executeRemoteCommand(String command) {
        return executeScript(command);
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

    public boolean executeScript(String command) {
        Process process = null;
        String status = "";
        logger.debug("Command to execute: {}", command);
        try {
            process = Runtime.getRuntime().exec(command);
        } catch (IOException e) {
            logger.error("Script failed to run : {}", e.getMessage());
            return false;
        }
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                status = line;
                System.out.println(line);
            }
        } catch (IOException e) {
            logger.error("Exception during reading console output : {}", e.getMessage());
            return false;
        }
        // Wait for the process to finish and get the exit code
        int exitCode = 1;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            logger.error("Script failed to run : {}", e.getMessage());
            return false;
        }

        // Check if the script was successful
        if (exitCode == 0) {
            logger.debug("Script executed successfully with exit code: {}", exitCode);
            return true;
        } else {
            logger.error("Script failed with exit code " + exitCode);
            return false;

        }
    }

    public boolean executeRemoteCommand(List<String> command) {
        ProcessBuilder processBuilder = new ProcessBuilder(command);
        processBuilder.redirectErrorStream(true);
        Process process = null;
        try {
            process = processBuilder.start();
        } catch (IOException e) {
            logger.error("Failed to execute script : {}", e.getMessage());
            return false;
        }

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
        } catch (IOException e) {
            logger.error("Failed to read console output : {}", e.getMessage());
        }

        int exitCode = 0;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            logger.error("Failed to retrieve  exitCode : {}", e.getMessage());
        }
        if (exitCode == 0) {
            System.out.println("Log file for test  downloaded successfully.");
        } else {
            System.err.println("Failed to download the log file for test . Exit code: " + exitCode);
        }
        return exitCode == 0;
    }

    protected String stripEtc(String fileName) {
        return fileName.contains("etc/jms-bridge/") ? fileName.replace("etc/jms-bridge/", "") : fileName;
    }

    protected Method[] getTestMethods(Class<?> clazz) {
        List<Method> testMethods = new ArrayList<>();
        Method[] methods = clazz.getDeclaredMethods();
        for (Method method : methods) {
            if (method.isAnnotationPresent(Test.class)) {
                testMethods.add(method);
            }
        }
        return testMethods.toArray(new Method[0]);
    }

    protected Method[] mergeTestMethods(Method[]... methodArrays) {
        int totalLength = 0;
        for (Method[] methods : methodArrays) {
            totalLength += methods.length;
        }
        Method[] mergedMethods = new Method[totalLength];
        int currentPos = 0;
        for (Method[] methods : methodArrays) {
            System.arraycopy(methods, 0, mergedMethods, currentPos, methods.length);
            currentPos += methods.length;
        }
        return mergedMethods;
    }

    public abstract void updateBrokerXMLFile();

    public abstract void updateConfigFile();

    protected void doUpdateBrokerXMLFile(String basePath, ConfigLoader configLoader) {
        String brokerFilePath = getAbsolutePath(basePath + stripEtc(configLoader.getBrokerXMLFile()));
        Method[] JMSQueueTestMethods = getTestMethods(JMSQueueTest.class);
        Method[] JMSTopicTestMethods = getTestMethods(JMSTopicTest.class);
        Method[] KafkaJMSTopicTestMethods = getTestMethods(KafkaJMSTopicTest.class);
        Method[] allTestMethods = mergeTestMethods(JMSQueueTestMethods, JMSTopicTestMethods, KafkaJMSTopicTestMethods);

        String kafkaTopicName = configLoader.getKafkaTopicName();
        boolean isJaasEnabled = configLoader.isJaasEnabled();
        boolean isSslEnabled = configLoader.isSslEnabled();

        String contentToBeChange;
        String tempFilePath = brokerFilePath + "_temp";
        try {
            Files.copy(Path.of(brokerFilePath), Path.of(tempFilePath));

            File xmlFile = new File(tempFilePath);
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();

            Document document = builder.parse(xmlFile);
            document.getDocumentElement().normalize();
            NodeList coreList = document.getElementsByTagName("core");


            Element coreElement = (Element) coreList.item(0);
            NodeList addressesList = document.getElementsByTagName("addresses");
            if (addressesList.getLength() > 0) {
                Element addressesElement = (Element) addressesList.item(0);
                coreElement.removeChild(addressesElement);
            }
            Element addressesElement = document.createElement("addresses");
            coreElement.appendChild(addressesElement);
            for (Method method : allTestMethods) {
                Element multicastAddress = document.createElement("address");
                multicastAddress.setAttribute("name", kafkaTopicName + "-" + method.getName());

                Element multicast = document.createElement("multicast");
                Element multicastQueue = document.createElement("queue");
                multicastQueue.setAttribute("name", kafkaTopicName + "-" + method.getName());
                multicast.appendChild(multicastQueue);
                multicastAddress.appendChild(multicast);

                addressesElement.appendChild(multicastAddress);
            }

            //Security Enabled block
            if (isJaasEnabled || isSslEnabled) {
                String findingElement = "security-enabled";
                //String contentToBeChange = "tcp://0.0.0.0:61616?isSslEnabled=true;keyStorePath=/home/xplocode/OM/Documents/JMSBridgePackages/jms-bridge-server-SSL/etc/jms-bridge/server-keystore.jks;keyStorePassword=securepass1";
                contentToBeChange = "tcp://0.0.0.0:61616?isSslEnabled=true;keyStorePath=" + getAbsolutePath(basePath + stripEtc(configLoader.getTrustStoreCertificateName())) + ";" + "keyStorePassword=" + configLoader.getTrustStorePassword();
                NodeList updateContent = document.getElementsByTagName(findingElement);
                Element updateElement;
                // Update the security enabled as true
                if (updateContent.getLength() > 0) {
                    updateElement = (Element) updateContent.item(0);
                    logger.info("Before change :" + updateElement.getTextContent());
                    if ("false".equals(updateElement.getTextContent())) {
                        updateElement.setTextContent("true");
                        logger.info("After change :" + updateElement.getTextContent());
                    } else {
                        logger.info("Security enabled");
                    }
                    // Update the value to true
                } else {
                    logger.info("<security-enabled> tag not found.");
                }
                // Changing the acceptor value
                if (isSslEnabled) {
                    findingElement = "acceptor";
                    updateContent = document.getElementsByTagName(findingElement);
                    if (updateContent.getLength() > 0) {
                        updateElement = (Element) updateContent.item(0);
                        updateElement.setTextContent(contentToBeChange);
                        logger.info("After Changing:" + updateElement.getTextContent());
                    } else {
                        logger.info("<" + findingElement + ">" + "tag not found.");
                    }
                }
            } else {
                logger.info("Security Enable is false");
            }
            //Write the updated content back to the XML file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(document);
            StreamResult result = new StreamResult(xmlFile);
            transformer.transform(source, result);
            uploadFileBashExecutor(tempFilePath, configLoader.getBrokerXMLFile());
        } catch (TransformerException | IOException | ParserConfigurationException | SAXException e) {
            logger.error("Error transforming the document: " + e.getMessage(), e);
        }
        try {
            Files.delete(Path.of(tempFilePath));
        } catch (IOException e) {
            logger.error("Error deleting temp file: " + e.getMessage(), e);
        }
    }

    protected abstract void uploadFileBashExecutor(String fileToUpload, String fileToBeReplacedInRemote);

    // Method to update the Kafka topic 'match' in the .conf file
    protected void doUpdateConfigFile(String basePath, ConfigLoader configLoader) {
        String configFilePath = getAbsolutePath(basePath + stripEtc(configLoader.getConfigFile()));
        String tempFilePath = configFilePath + "_temp";

        logger.debug("Config file path: {} ", configFilePath);
        Path configPath = Paths.get(configFilePath);
        if (!Files.exists(configPath)) {
            logger.error("Config file does not exist: {} ", configFilePath);
            return;
        }
        String keyToUpdate = "match";
        String currentKafkaAddress = null;
        String newValue = configLoader.getKafkaTopicName();
        logger.debug("New value: {}", newValue);
        Path tempPath = Paths.get(tempFilePath);
        try {
            List<String> lines = Files.readAllLines(configPath);
            for (String s : lines) {
                String line = s.trim();
                if (line.startsWith(keyToUpdate)) {
                    currentKafkaAddress = line.split("=")[1].trim().replaceAll("\"", "");
                    logger.debug("Current value: {}", currentKafkaAddress);
                    break;
                }
            }
            if (newValue != null && !newValue.equals(currentKafkaAddress)) {
                int i = 0;
                int lineSize = lines.size();
                for (; i < lineSize; i++) {
                    String line = lines.get(i).trim();
                    if (line.startsWith(keyToUpdate)) {
                        lines.set(i, keyToUpdate + " = \"" + newValue + "*\"");
                        logger.debug("Updating line: {}", lines.get(i));
                        break;
                    }
                }
                if (i == lineSize) {
                    logger.debug("Match property is not found, so file is not updated.");
                    return;
                }
                Files.write(tempPath, lines);
                logger.debug("Config updated with new value: {}", newValue);
            } else {
                logger.debug("No new value found or value remains the same.");
                return;
            }

        } catch (IOException e) {
            logger.error("Error updating config file: {}", e.getMessage(), e);
            return;
        }
        if (Files.exists(tempPath)) {
            uploadFileBashExecutor(tempFilePath, configLoader.getConfigFile());
            try {
                Files.delete(tempPath);
            } catch (IOException e) {
                logger.error("Error deleting temp file: " + e.getMessage(), e);
            }
        }
    }


    public boolean isServerUp(ServerType serverType, int retryIntervalInSeconds, int maxRetries) {
        maxRetries = maxRetries < 1 ? 1 : 3;
        Pair<String, Integer> hostPort = getHostPortPairFromConfig(serverType);
        boolean isSuccess = false;
        for (int i = 0; i < maxRetries; i++) {
            isSuccess = isPortOpen(hostPort.getLeft(), hostPort.getRight());
            if (isSuccess) {
                Util.addSteps(steps, serverType.toString() + " Server now live");
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
            Util.addSteps(steps, serverType.toString() + " Server did not start within the expected time.");
        }
        return isSuccess;
    }

    protected abstract Pair<String, Integer> getHostPortPairFromConfig(ServerType serverType);

    public boolean isPortOpen(String host, int port) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 2000); // Timeout is 2000 milliseconds
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public abstract ConfigLoader getConfigLoader();

    /**
     * Kills jms bridge instances if running - in case previous test was aborted / didnt cleanup
     * @return script execution success / failure
     */
    public abstract boolean resetInstances() ;

    /**
     * Resets kafka container state and streams state directories
     * @return script execution success / failure
     */
    public abstract boolean resetKafkaAndLocalState() ;

}