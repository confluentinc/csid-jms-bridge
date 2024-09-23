package io.confluent.jms.bridge.util;

import io.confluent.jms.bridge.server.ServerConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.lang.reflect.Method;
import java.util.Optional;

import static io.confluent.jms.bridge.util.Util.steps;

public class JBTestWatcher implements TestWatcher, BeforeEachCallback, AfterEachCallback {

    private static final Logger logger = LogManager.getLogger(JBTestWatcher.class);

    private final String sheetName;

    public JBTestWatcher(String sheetName) {
        this.sheetName = sheetName;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        steps.setLength(0);
        ServerConfig.ServerSetup.resetKafkaAndLocalState();
        String methodName = context.getTestMethod().map(Method::getName).orElse("Unknown Method");
        String className = context.getTestClass().map(Class::getName).orElse("Unknown Class");
        logger.info("Starting test: {} in class: {}", methodName, className);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        String methodName = context.getTestMethod().map(Method::getName).orElse("Unknown Method");
        String className = context.getTestClass().map(Class::getName).orElse("Unknown Class");
        logger.info("Finished test: {} in class: {}", methodName, className);
        resetServerState();
    }
    @Override
    public void testSuccessful(ExtensionContext context) {
        logTestCaseResult(context, null);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        logTestCaseResult(context, cause);
    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
    }

    private void resetServerState() {
        logger.info("Resetting server state");
        Util.isDownloadLog = false;
        ServerConfig.ServerSetup.stopSlaveServer();
        ServerConfig.ServerSetup.stopMasterServer();
        //steps.setLength(0);
    }

    private void logTestCaseResult(ExtensionContext context, Throwable cause) {
        String testCaseName = context.getTestMethod().map(Method::getName).orElse(context.getDisplayName());
        String className = context.getTestClass().map(Class::getName).orElse("Unknown Class");
        logger.debug("Steps: {}", steps);
        if(cause == null) {
            logger.info("Test passed: {} in class: {}", testCaseName, className);
            ExcelUtil.logTestCaseResult(sheetName, testCaseName, String.valueOf(steps), true, null);
        } else {
            logger.info("Test failed: {} in class: {} with reason: {}", testCaseName,className, cause.getMessage());
            ExcelUtil.logTestCaseResult(sheetName, testCaseName, String.valueOf(steps),false, cause.getMessage());
        }
    }
}

