package io.psyncopate.util;

import lombok.Getter;
import lombok.experimental.FieldDefaults;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@FieldDefaults(makeFinal = true)
public class RemoteConfigLoader extends ConfigLoader {
    @Getter
    private final String serverMasterExecutionPath;
    @Getter
    private String serverMasterUser;

    @Getter
    private String serverMasterPemPath;
    @Getter
    private String serverSlaveExecutionPath;
    @Getter
    private String serverSlaveUser;

    @Getter
    private String serverSlavePemPath;


    public RemoteConfigLoader() {
        super("application.remote.properties");

        this.serverMasterExecutionPath = properties.getProperty("server.master.executionPath");
        this.serverMasterUser = properties.getProperty("server.master.user");
        this.serverMasterPemPath = properties.getProperty("server.master.pemPath");

        this.serverSlaveExecutionPath = properties.getProperty("server.slave.executionPath");
        this.serverSlaveUser = properties.getProperty("server.slave.user");
        this.serverSlavePemPath = properties.getProperty("server.slave.pemPath");
    }

}
