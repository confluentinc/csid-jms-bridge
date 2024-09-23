package io.confluent.jms.bridge;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class Config {
    private  String executionPath;
    private  String command;
    private  String background;
    private  String configFile;
    private  String serverMasterHost;
    private  String serverMasterUser;
    private  String serverMasterPort;
    private  String serverMasterPemDirectory;
    private  String serverSlavePort;
    private  String serverSlaveHost;
    private  String serverSlaveUser;
    private  String serverSlavePemDirectory;

    public Config(String executionPath, String command, String background, String configFile, String serverMasterName, String serverSlaveName, String serverMasterPort, String serverSlavePort) {
        this.executionPath = executionPath;
        this.command = command;
        this.background = background;
        this.configFile = configFile;
        this.serverMasterHost = serverMasterName;
        this.serverMasterPort = serverMasterPort;
        this.serverSlaveHost = serverSlaveName;
        this.serverSlavePort = serverSlavePort;

    }
}
