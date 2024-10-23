package io.psyncopate.server;

import io.psyncopate.util.LocalConfigLoader;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.Constants;
import io.psyncopate.util.constants.ServerType;
import lombok.Data;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.List;

import static io.psyncopate.util.Util.getLogDirectoryPath;

public class LocalServerControl extends ServerControl {
    private static final String SCRIPT_BASE_PATH = "src/test/resources/scripts/local/";

    private final Logger logger = LogManager.getLogger(ServerControl.class);
    @Getter
    private final LocalConfigLoader configLoader;

    public LocalServerControl(LocalConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    public boolean resetInstances() {
        String scriptPath = SCRIPT_BASE_PATH + "reset-instances.sh";
        return executeScript(getAbsolutePath(scriptPath));
    }
    public boolean resetKafkaAndLocalState() {
        String scriptPath = SCRIPT_BASE_PATH + "reset-state.sh";
        return executeScript(getAbsolutePath(scriptPath));
    }
    public boolean startServer(boolean isMaster) {
        String scriptPath = SCRIPT_BASE_PATH + "server-start.sh";
        return executeScript(getArgsByNode(scriptPath, isMaster, true, false));
    }

    private List<String> getArgsByNode(String scriptPath, boolean isMaster, boolean isStartCmd, boolean killServer) {
        String scriptAbsolutePath = getAbsolutePath(scriptPath);
        String name = isMaster ? configLoader.getServerMasterName() : configLoader.getServerSlaveName();
        String port = isMaster ? configLoader.getServerMasterAppPort() : configLoader.getServerSlaveAppPort();
        String command = isStartCmd ? configLoader.getStartCommand() : configLoader.getStopCommand();
        String path = isMaster ? configLoader.getServerMasterExecutionPath() : configLoader.getServerSlaveExecutionPath();

        String mode = isMaster ? Constants.ACTIVE : Constants.STANDBY;
        String logFilePath = getLogDirectoryPath() + "/server_command.log";
        String kill = killServer ? "Kill" : "";
        if (isStartCmd) {
            logger.info("Executing command: {}", concatenateStrings(" ", scriptAbsolutePath, name, port, path, command, mode, logFilePath));
            return Arrays.asList(scriptAbsolutePath, name, port, path, command, mode, logFilePath);
        } else {
            logger.info("Executing command: {}", concatenateStrings(" ", scriptAbsolutePath, name, port, path, command, logFilePath, kill));
            return Arrays.asList(scriptAbsolutePath, name, port, path, command, logFilePath, kill);
        }
    }

    public boolean stopServer(boolean isMaster, boolean killServer) {
        String scriptPath = SCRIPT_BASE_PATH + "server-stop.sh";
        return executeScript(getArgsByNode(scriptPath, isMaster, false, killServer));
    }

    public boolean downloadLog(String testcaseName, boolean isMaster) {
        String logPath = isMaster ? configLoader.getServerMasterExecutionPath() : configLoader.getServerSlaveExecutionPath();
        String remoteLogFile = logPath + "/logs/jms-bridge.out";
        String localFilePath = Util.getTestcaseFilePath(testcaseName, isMaster); // Store log with test name
        List<String> command = Arrays.asList("cp", remoteLogFile, localFilePath);
        return executeScript(command);
    }

    public boolean executeScript(String command) {
        //String result = executeCommand(command);
        //return ServerStatus.valueOf(result.toUpperCase());
        return executeCommand(command);
    }

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


    @Override
    public void updateBrokerXMLFile() {
        doUpdateBrokerXMLFile(SCRIPT_BASE_PATH, configLoader);
    }

    public void updateConfigFile() {
        doUpdateConfigFile(SCRIPT_BASE_PATH, configLoader);
    }


    // Method to execute a bash script to update EC2 configuration
    @Override
    public void uploadFileBashExecutor(String fileToUpload, String fileToBeReplacedInRemote) {
        try {
            //Master server properties

            String serverMasterExecutionPath = configLoader.getServerMasterExecutionPath();
            String remoteConfigPath = serverMasterExecutionPath + "/" + fileToBeReplacedInRemote;

            // Executing the Bash script to change the configs on the master server
            executeScript(prepareCommandToExecuteScript(fileToUpload, remoteConfigPath));

            // Slave server's properties


            String serverSlaveExecutionPath = configLoader.getServerSlaveExecutionPath();

            remoteConfigPath = serverSlaveExecutionPath + fileToBeReplacedInRemote;

            logger.debug("Slave remote file path :{}", remoteConfigPath);

            if (!fileToUpload.contains(".xml")) {

                executeScript(prepareCommandToExecuteScript(fileToUpload, remoteConfigPath));
            }

        } catch (Exception e) {
            logger.error("Error executing Bash script: " + e.getMessage(), e);
        }
    }

    @Override
    protected Pair<String, Integer> getHostPortPairFromConfig(ServerType serverType) {
        return serverType == ServerType.MASTER ?
                Pair.of(configLoader.getServerMasterHost(), Integer.parseInt(configLoader.getServerMasterAppPort()))
                : Pair.of(configLoader.getServerSlaveHost(), Integer.parseInt(configLoader.getServerSlaveAppPort()));
    }

    private static String prepareCommandToExecuteScript(String localConfigFile, String remoteConfigPath) {
        String logFilePath = Util.getLogDirectoryPath() + "/server_command.log";
        String scriptAbsolutePath = SCRIPT_BASE_PATH + "update_config.sh";
        return concatenateStrings(" ", scriptAbsolutePath, localConfigFile, remoteConfigPath, logFilePath);
    }
}
