package io.psyncopate;

import io.psyncopate.server.LocalServerControl;
import io.psyncopate.server.RemoteServerControl;
import io.psyncopate.server.ServerControl;
import io.psyncopate.server.ServerSetup;
import io.psyncopate.util.ConfigLoader;
import io.psyncopate.util.LocalConfigLoader;
import io.psyncopate.util.RemoteConfigLoader;
import io.psyncopate.util.constants.Constants;
import lombok.NonNull;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

import javax.jms.Topic;

public class GlobalSetup implements BeforeAllCallback {
    private static final Logger logger = LogManager.getLogger(GlobalSetup.class);
    private static boolean isInitialized = false;
    private static ServerSetup serverSetupInstance;
    private static ConfigLoader configLoaderInstance;
    private final Constants.TestExecutionMode executionMode = Constants.TestExecutionMode.LOCAL;

    public static ServerSetup getServerSetup() {
        if (!isInitialized) {
            throw new IllegalStateException("Global setup not initialized yet.");
        }
        return serverSetupInstance;
    }

    @NonNull
    public static ConfigLoader getConfligLoader() {
        if (!isInitialized) {
            throw new IllegalStateException("Global setup not initialized yet.");
        }
        return configLoaderInstance;
    }

    @Override
    public void beforeAll(ExtensionContext context) {
        if (!isInitialized) {
            logger.info("Global setting up configurations.");
            ServerControl serverControl;
            if (executionMode == Constants.TestExecutionMode.LOCAL) {
                configLoaderInstance = new LocalConfigLoader();
                serverControl = new LocalServerControl((LocalConfigLoader) configLoaderInstance);
            } else {
                configLoaderInstance = new RemoteConfigLoader();
                serverControl = new RemoteServerControl((RemoteConfigLoader) configLoaderInstance);
            }
            serverSetupInstance= new ServerSetup(serverControl, executionMode);
            serverControl.updateConfigFile();
            serverControl.updateBrokerXMLFile();
            // Start container or other global setup
            isInitialized = true;
        } else {
            logger.info("Global setup already initialized, skipping...");
        }
    }
}
