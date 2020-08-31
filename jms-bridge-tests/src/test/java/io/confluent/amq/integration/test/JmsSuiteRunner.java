/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.ClassFileLocator;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;
import net.bytebuddy.pool.TypePool;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.activemq.artemis.tests.integration.jms.RedeployTempTest;
import org.apache.activemq.artemis.tests.integration.jms.largemessage.JMSLargeMessageTest;
import org.apache.activemq.artemis.tests.util.JMSTestBase;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.runner.Description;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.testcontainers.containers.KafkaContainer;

public class JmsSuiteRunner extends Suite {
  //
  // Look into overriding this static method with byte buddy as an alternative
  /*
     public static ActiveMQServer newActiveMQServer(final Configuration config,
                                                  final MBeanServer mbeanServer,
                                                  final ActiveMQSecurityManager securityManager,
                                                  final boolean enablePersistence) {
      config.setPersistenceEnabled(enablePersistence);

      ActiveMQServer server = new ActiveMQServerImpl(config, mbeanServer, securityManager);

      return server;
   }

   */
  static {
    ByteBuddyAgent.install();
    new ByteBuddy()
        .redefine(ActiveMQServers.class)
        .method(named("newActiveMQServer").and(takesArguments(4)))
        .intercept(MethodDelegation.to(ActiveMQServersRedefined.class))
        .make()
        .load(ActiveMQServers.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());

//    new ByteBuddy()
//        .redefine(MyJmsTestBase.class)
//        .name(JMSTestBase.class.getName())
//        .make()
//        .load(MyJmsTestBase.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());
  }

  private static final String JMS_PACKAGE = "org.apache.activemq.artemis.tests.integration.jms";
  private static final Set<String> EXCLUDED_TEST_CLASSES = new HashSet<>(Arrays.asList(
      //The below test does not work with the default ActiveMQ implementation
      //---------------------------------------------------------------------
      "RedeployTempTest",

      //These are being excluded because they test features not supported by the kafka backend
      //--------------------------------------------------------------------------------------
      "JMSLargeMessageTest"
  ));
  private static final Set<String> EXCLUDED_TESTS = new HashSet<>(Arrays.asList(
      //these below do not work with the default ActiveMQ implementation
      //----------------------------------------------------------------
      "SimpleJNDIClientTest#testRemoteCFWithUDP",
      "SimpleJNDIClientTest#testConnectionFactoryStringWithInvalidParameter",
      "ActiveMQConnectionFactoryTest#testDiscoveryConstructor",
      "ManualReconnectionToSingleServerTest#testExceptionListener",

      //These are being excluded because they test features not supported by the kafka backend
      //--------------------------------------------------------------------------------------
      //--The below require large message support
      "JMSLargeMessageTest#testResendWithLargeMessage",
      "TextMessageTest#testSendReceiveWithBody0xffffplus1",
      "TextMessageTest#testSendReceiveWithBody0xfffftimes2",
      "TextMessageTest#testSendReceiveWithBody0xffff",
      "ReSendMessageTest#testResendWithLargeMessage"
  ));
  private static final Set<String> EXCLUDED_PARAMERATIZED_TESTS = new HashSet<>(Arrays.asList(
      "[persistenceEnabled = AMQP]"
  ));

  private static Class<?>[] scanClasses() {
    return ReflectionSupport
        .findAllClassesInPackage(
            JMS_PACKAGE,
            clz -> !EXCLUDED_TEST_CLASSES.contains(clz.getSimpleName()),
            name -> name.endsWith("Test"))
        .toArray(new Class<?>[0]);
  }

  protected KafkaContainer kafkaContainer;

  public JmsSuiteRunner(Class<?> klass, RunnerBuilder builder) throws InitializationError {
    super(klass, scanClasses());
    try {
      filter(new JmsTestFilter());
    } catch (NoTestsRemainException e) {
      throw new InitializationError(e);
    }
  }

  public static class JmsTestFilter extends org.junit.runner.manipulation.Filter {

    @Override
    public boolean shouldRun(Description description) {
      Class<?> testClass = description.getTestClass();

      boolean runIt = true;
      if (testClass != null) {
        runIt = testClass.getPackage().getName().startsWith(JMS_PACKAGE)
            && shouldRunClassMethod(testClass, description.getMethodName())
            && inheritesFromJmsBaseTest(testClass);
      } else if (description.getDisplayName().matches("\\[.+]")) {
        //paramterized test
        runIt = !EXCLUDED_PARAMERATIZED_TESTS.contains(description.getDisplayName());
      }

      if (!runIt) {
        System.out.println(String.format("%s: Excluding test %s",
            this.getClass().getSimpleName(),
            description.getDisplayName()));
      }

      return runIt;
    }

    @Override
    public String describe() {
      return "JMS Tests";
    }

    boolean shouldRunClassMethod(Class<?> clazz, String methodName) {
      String matchAgainst = clazz.getSimpleName() + "#" + methodName;
      return !EXCLUDED_TEST_CLASSES.contains(clazz.getSimpleName())
          && !EXCLUDED_TESTS.contains(matchAgainst);

    }

    boolean inheritesFromJmsBaseTest(Class<?> testClass) {
      //excluding all test cases that aren't using our overidden test base
      return JMSTestBase.class.isAssignableFrom(testClass);
    }
  }
}
