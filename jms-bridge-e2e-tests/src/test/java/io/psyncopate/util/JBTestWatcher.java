package io.psyncopate.util;

import io.psyncopate.GlobalSetup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.psyncopate.server.ServerSetup;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.TestWatcher;

import java.lang.reflect.Method;
import java.util.Optional;

import static io.psyncopate.util.Util.steps;

public class JBTestWatcher implements TestWatcher, BeforeEachCallback, AfterEachCallback {

    private static final Logger logger = LogManager.getLogger(JBTestWatcher.class);

    private final String sheetName;

    public JBTestWatcher(String sheetName) {
        this.sheetName = sheetName;
    }

    @Override
    public void beforeEach(ExtensionContext context) {
        steps.setLength(0);
        GlobalSetup.getServerSetup().resetInstances();

        String methodName = context.getTestMethod().map(Method::getName).orElse("Unknown Method");
        String className = context.getTestClass().map(Class::getName).orElse("Unknown Class");
        logger.info("Starting test: {} in class: {}", methodName, className);
    }

    @Override
    public void afterEach(ExtensionContext context) {
        // Get the test class name
        String className = context.getRequiredTestClass().getSimpleName();
        logger.info("Test class name: {}", className);
        resetServerState();
        GlobalSetup.getServerSetup().resetKafkaAndLocalState();
        String methodName = context.getTestMethod().map(Method::getName).orElse("Unknown Method");
        String classFullName = context.getTestClass().map(Class::getName).orElse("Unknown Class");
        logger.info("Finished test: {} in class: {}", methodName, classFullName);

    }
    @Override
    public void testSuccessful(ExtensionContext context) {
        logTestCaseResult(context, null);
    }

    @Override
    public void testFailed(ExtensionContext context, Throwable cause) {
        logTestCaseResult(context, cause);
        if(Util.isDownloadLog) {
            return;
        }
        String testCaseName = context.getTestMethod().map(Method::getName).orElse(context.getDisplayName());
        if (cause instanceof org.opentest4j.AssertionFailedError) {
            String failureMessage = cause.getMessage();
            logger.debug("Test failed: {} with assertion failure: {}", testCaseName, failureMessage);
            downloadLog(testCaseName, failureMessage);
        }

    }

    @Override
    public void testAborted(ExtensionContext context, Throwable cause) {
    }

    @Override
    public void testDisabled(ExtensionContext context, Optional<String> reason) {
    }

    private void downloadLog(String testCaseName, String message) {
        if (message.contains("Master Server should start")) {
            Util.downloadLog(testCaseName, true);
        } else if (message.contains("Slave Server should start")) {
            Util.downloadLog(testCaseName, false);
        } else if (message.contains("Number of sent and received messages should match")) {
        	
        } else if (message.contains("Number of received messages should not be zero")){
            Util.downloadLog(testCaseName, true);
            Util.downloadLog(testCaseName, false);
        }
        else if (message.contains("Slave Server should be running")) {
        	Util.downloadLog(testCaseName, false);
        }
    }

    private void resetServerState() {
        logger.info("Resetting server state");
        Util.isDownloadLog = false;
        GlobalSetup.getServerSetup().stopSlaveServer();
        GlobalSetup.getServerSetup().stopMasterServer();

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

