package io.psyncopate.server;

import io.psyncopate.util.RemoteConfigLoader;
import io.psyncopate.util.Util;
import io.psyncopate.util.constants.Constants;
import io.psyncopate.util.constants.ServerType;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

public class RemoteServerControl extends ServerControl {
    private static final String SCRIPT_BASE_PATH = "src/test/resources/scripts/remote/";

    private static final Logger logger = LogManager.getLogger(RemoteServerControl.class);

    @Getter
    private final RemoteConfigLoader configLoader;

    public RemoteServerControl(RemoteConfigLoader configLoader) {
        this.configLoader = configLoader;
    }

    public boolean startServer(boolean isMaster) {
        String scriptPath = SCRIPT_BASE_PATH + "server-start.sh";
        return executeRemoteCommand(getArgsByNode(scriptPath, isMaster, true, false));
    }

    private String getArgsByNode(String scriptPath, boolean isMaster, boolean isStartCmd, boolean killServer) {

        String scriptAbsolutePath = getAbsolutePath(scriptPath);

        String host = isMaster ? configLoader.getServerMasterHost() : configLoader.getServerSlaveHost();
        String user = isMaster ? configLoader.getServerMasterUser() : configLoader.getServerSlaveUser();
        String port = isMaster ? configLoader.getServerMasterAppPort() : configLoader.getServerSlaveAppPort();
        String executionPath = isMaster ? configLoader.getServerMasterExecutionPath() : configLoader.getServerSlaveExecutionPath();
        String command = isStartCmd ? configLoader.getStartCommand() : configLoader.getStopCommand();
        String configFile = configLoader.getConfigFile();
        String pemPath = isMaster ? configLoader.getServerMasterPemPath() : configLoader.getServerSlavePemPath();
        String mode = isMaster ? Constants.ACTIVE : Constants.STANDBY;
        String logFilePath = Util.getLogDirectoryPath() + "/server_command.log";
        String kill = killServer ? "Kill" : "";
        if (isStartCmd) {
            return concatenateStrings(" ", scriptAbsolutePath, host, port, executionPath, command, configFile, mode, user, pemPath, logFilePath);
        } else {
            return concatenateStrings(" ", scriptAbsolutePath, host, port, executionPath, command, user, pemPath, logFilePath, kill);
        }
    }

    public boolean stopServer(boolean isMaster, boolean killServer) {
        String scriptPath = SCRIPT_BASE_PATH + "server-stop.sh";
        return executeRemoteCommand(getArgsByNode(scriptPath, isMaster, false, killServer));

    }

    public boolean downloadLog(String testcaseName, boolean isMaster) {
        String user = isMaster ? configLoader.getServerMasterUser() : configLoader.getServerSlaveUser();
        String host = isMaster ? configLoader.getServerMasterHost() : configLoader.getServerSlaveHost();
        String executionPath = isMaster ? configLoader.getServerMasterExecutionPath() : configLoader.getServerSlaveExecutionPath();
        String pemPath = isMaster ? configLoader.getServerMasterPemPath() : configLoader.getServerSlavePemPath();
        String remoteLogFile = "/home/" + user + "/" + executionPath + "logs/jms-bridge.out";
        String localFilePath = Util.getTestcaseFilePath(testcaseName, isMaster); // Store log with test name
        List<String> command = Arrays.asList("scp", "-i", pemPath, user + "@" + host + ":" + remoteLogFile, localFilePath);
        return executeRemoteCommand(command);
    }

    @Override
    public void updateBrokerXMLFile() {
        doUpdateBrokerXMLFile(SCRIPT_BASE_PATH,configLoader);
    }

    public void updateConfigFile() {
        doUpdateConfigFile(SCRIPT_BASE_PATH,configLoader);
    }


    // Method to execute a bash script to update EC2 configuration
    @Override
    public void uploadFileBashExecutor(String fileToUpload, String fileToBeReplacedInRemote) {
        try {
            //Master server properties
            String masterPemFile = configLoader.getServerMasterPemPath();
            String masterUser = configLoader.getServerMasterUser();
            String masterHost = configLoader.getServerMasterHost();
            String serverMasterExecutionPath = configLoader.getServerMasterExecutionPath();
            String remoteConfigPath = serverMasterExecutionPath + fileToBeReplacedInRemote;

            // Executing the Bash script to change the configs on the master server
            executeScript(prepareCommandToExecuteScript(masterPemFile, masterUser, masterHost, fileToUpload, remoteConfigPath));

            // Slave server's properties
            String slavePemFile = configLoader.getServerSlavePemPath();
            String slaveUser = configLoader.getServerSlaveUser();
            String slaveHost = configLoader.getServerSlaveHost();

            String serverSlaveExecutionPath = configLoader.getServerSlaveExecutionPath();

            remoteConfigPath = serverSlaveExecutionPath + fileToBeReplacedInRemote;

            logger.debug("Slave remote file path :{}", remoteConfigPath);

            if (!fileToUpload.contains(".xml")) {

                executeScript(prepareCommandToExecuteScript(slavePemFile, slaveUser, slaveHost, fileToUpload, remoteConfigPath));
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

    @Override
    public boolean resetInstances() {
        //NOOP for remote at least for now - should kill jms bridge instances if running - in case previous test didnt do clean up etc.
        return true;
    }

    @Override
    public boolean resetKafkaAndLocalState() {
        //NOOP for remote at least for now - should reset kafka container and streams state directories
        return true;
    }

    private static String prepareCommandToExecuteScript(String pemFile, String user, String host, String localConfigFile, String remoteConfigPath) {

        String logFilePath = Util.getLogDirectoryPath() + "/server_command.log";
        String scriptAbsolutePath = SCRIPT_BASE_PATH + "update_ec2_config.sh";
        return concatenateStrings(" ", scriptAbsolutePath, pemFile, user, host, localConfigFile, remoteConfigPath, logFilePath);
    }

}