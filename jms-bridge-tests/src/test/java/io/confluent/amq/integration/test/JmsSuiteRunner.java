/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;

import io.confluent.amq.JmsBridgeConfiguration;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.agent.ByteBuddyAgent;
import net.bytebuddy.dynamic.loading.ClassReloadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.server.ActiveMQServers;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.junit.internal.builders.AllDefaultPossibilitiesBuilder;
import org.junit.platform.commons.support.ReflectionSupport;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runner.Runner;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunListener;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.JUnit4;
import org.junit.runners.Suite;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.junit.runners.model.TestClass;
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
    //rebase this class
    // InVMNodeManagerServer extends ActiveMQServerImpl
    ByteBuddyAgent.install();
    new ByteBuddy()
        .redefine(ActiveMQServers.class)
        .method(named("newActiveMQServer").and(takesArguments(4)))
        .intercept(MethodDelegation.to(ActiveMQServersRedefined.class))
        .make()
        .load(ActiveMQServers.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());

//    new ByteBuddy()
//        .redefine(ActiveMQTestBase.class)
//        .visit(Advice.to(JmsFailoverTestRedefined.class).on(named("addServer")))
//        .make()
//        .load(ActiveMQTestBase.class.getClassLoader(), ClassReloadingStrategy.fromInstalledAgent());

  }

  private static final String JMS_PACKAGE = "org.apache.activemq.artemis.tests.integration.jms";
  private static final String FAILOVER_PACKAGE = "org.apache.activemq.artemis.tests.integration.cluster.failover";
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
  private static final Set<String> INCLUDED_TESTS = new HashSet<>(Arrays.asList(
      "testTransactedMessagesSentSoRollbackAndContinueWork",
      ""
  ));
  private static final Set<String> EXCLUDED_PARAMERATIZED_TESTS = new HashSet<>(Arrays.asList(
      "[persistenceEnabled = AMQP]"
  ));

  public static AtomicInteger BRIDGE_ID_SEQUENCE = new AtomicInteger(1);
  public static AtomicInteger NODED_ID_SEQUENCE = new AtomicInteger(1);

  public static TemporaryFolder temporaryFolder = new TemporaryFolder();

  public static KafkaContainer kafkaContainer =
      new KafkaContainer("5.4.0")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false");

  public static JmsBridgeConfiguration wrapConfig(Configuration amqConfig) {
    try {
      Properties kafkaProps = new Properties();
      String bridgeId = "unit-test-" + BRIDGE_ID_SEQUENCE.get();
      kafkaProps.setProperty(
          ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
          kafkaContainer.getBootstrapServers());
      kafkaProps.setProperty("bridge.id", bridgeId);
      kafkaProps.setProperty(
          StreamsConfig.STATE_DIR_CONFIG,
          temporaryFolder
              .newFolder(bridgeId + "-" + NODED_ID_SEQUENCE.incrementAndGet()).getAbsolutePath());
      kafkaProps.setProperty(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "500");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG), "6000");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG), "2000");
      kafkaProps.setProperty(
          StreamsConfig.consumerPrefix(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG), "5000");
      kafkaProps.setProperty(
          StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1");

      JmsBridgeConfiguration jmsBridgeConfiguration = new JmsBridgeConfiguration(amqConfig,
          kafkaProps);

      return jmsBridgeConfiguration;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

  }


  @SuppressWarnings("checkstyle:BooleanExpressionComplexity")
  private static Class<?>[] scanClasses(Class<?> suiteClass) throws InitializationError {
    List<Class<?>> results = new LinkedList<>();

    JmsSuitePackages annotation = suiteClass.getAnnotation(JmsSuitePackages.class);
    if (annotation == null) {
      throw new InitializationError(
          String.format("class '%s' must have a SuitePackages annotation", suiteClass.getName()));
    }

    Set<String> excludePackages = annotation.excludePackages() != null
        ? new HashSet<>(Arrays.asList(annotation.excludePackages()))
        : Collections.emptySet();

    Set<Class<?>> includeClasses = annotation.includeClasses() != null
        ? new HashSet<>(Arrays.asList(annotation.includeClasses()))
        : Collections.emptySet();

    Set<Class<?>> excludeClasses = annotation.excludeClasses() != null
        ? new HashSet<>(Arrays.asList(annotation.excludeClasses()))
        : Collections.emptySet();

    Stream.of(annotation.includePackages()).forEach(pkg ->
        results.addAll(ReflectionSupport
            .findAllClassesInPackage(
                pkg,
                clz -> (excludePackages.isEmpty() || !excludePackages
                    .contains(clz.getPackage().getName()))
                    && (includeClasses.isEmpty() || includeClasses.contains(clz))
                    && (excludeClasses.isEmpty() || !excludeClasses.contains(clz)),
                name -> name.endsWith("Test"))));

    return results.toArray(new Class<?>[0]);
  }

  public JmsSuiteRunner(Class<?> klass, RunnerBuilder builder) throws InitializationError {
    super(new CustomRunnerBuilder(), klass, scanClasses(klass));

    JmsSuitePackages annotation = klass.getAnnotation(JmsSuitePackages.class);
    if (annotation == null) {
      throw new InitializationError(
          String.format("class '%s' must have a SuitePackages annotation", klass.getName()));
    }
    try {
      filter(new JmsTestFilter(annotation.includeTests(), annotation.excludeTests()));
    } catch (NoTestsRemainException e) {
      throw new InitializationError(e);
    }
  }

  @Override
  protected List<TestRule> classRules() {
    return Arrays.asList(kafkaContainer, temporaryFolder);
  }

  @Override
  protected void runChild(Runner runner, RunNotifier notifier) {
    notifier.addFirstListener(new RunListener() {
      @Override
      public void testStarted(Description description) throws Exception {
        BRIDGE_ID_SEQUENCE.incrementAndGet();
      }
    });
    super.runChild(runner, notifier);
  }


  public static class JmsTestFilter extends org.junit.runner.manipulation.Filter {

    final Set<String> includeTests;
    final Set<String> excludeTests;

    public JmsTestFilter(String[] includeTests, String[] excludeTests) {
      this.includeTests = includeTests  != null
          ? new HashSet<>(Arrays.asList(includeTests))
          : Collections.emptySet();
      this.excludeTests = excludeTests != null
          ? new HashSet<>(Arrays.asList(excludeTests))
          : Collections.emptySet();
    }

    @Override
    public boolean shouldRun(Description description) {
      Class<?> testClass = description.getTestClass();

      boolean runIt = shouldRunClassMethod(testClass, description.getMethodName());
      if (runIt && description.getDisplayName().matches("\\[.+]")) {
        //paramterized test
        runIt = !EXCLUDED_PARAMERATIZED_TESTS.contains(description.getDisplayName());
      }

      if (!runIt) {
        System.out.println(String.format("%s: Excluding test %s#%s",
            this.getClass().getSimpleName(),
            description.getDisplayName(),
            description.getMethodName()));
      }

      return runIt;
    }

    @Override
    public String describe() {
      return "JMS Tests";
    }

    boolean shouldRunClassMethod(Class<?> clazz, String methodName) {
      if (clazz == null || methodName == null) {
        return true;
      }
      String matchAgainst = clazz.getSimpleName() + "#" + methodName;
      return (includeTests.isEmpty() || includeTests.contains(matchAgainst))
          && (excludeTests.isEmpty() || !excludeTests.contains(matchAgainst));
    }

    boolean inheritesFromJmsBaseTest(Class<?> testClass) {
      //excluding all test cases that aren't using our overidden test base
      return testClass.getSimpleName().equals("FailoverTest");
    }
  }

  public static class CustomRunnerBuilder extends AllDefaultPossibilitiesBuilder {


    @Override
    public Runner runnerForClass(Class<?> testClass) throws Throwable {

      Runner runner = super.runnerForClass(testClass);
      if (runner instanceof JUnit4) {
        return new CustomRunner(testClass);
      } else {
        return runner;
      }
    }
  }

  public static class CustomRunner extends BlockJUnit4ClassRunner {

    public CustomRunner(Class<?> testClass) throws InitializationError {
      super(testClass);
    }

    public CustomRunner(TestClass testClass) throws InitializationError {
      super(testClass);
    }

    @Override
    protected List<TestRule> getTestRules(Object target) {

      return super.getTestRules(target);
    }
    //    @Override
//    protected boolean isIgnored(FrameworkMethod child) {
//      return !INCLUDED_TESTS.contains(child.getMethod().getName());
//    }
  }

  /**
   * The <code>SuitePackages</code> annotation specifies the classes to be run when a class
   * annotated with <code>@RunWith(JmsSuiteRunner.class)</code> is run.
   */
  @Retention(RetentionPolicy.RUNTIME)
  @Target(ElementType.TYPE)
  @Inherited
  public @interface JmsSuitePackages {

    /**
     * @return the packages to be run
     */
    String[] includePackages();

    /**
     * @return the sub packages to be excluded
     */
    String[] excludePackages();

    /**
     * @return list of classes to include from the packages
     */
    Class<?>[] includeClasses();

    /**
     * @return list of classes to exclude from the packages
     */
    Class<?>[] excludeClasses();

    /**
     * @return list of tests to include from the classes
     */
    String[] includeTests();

    /**
     * @return list of tests to exclude from the classes
     */
    String[] excludeTests();
  }
}
