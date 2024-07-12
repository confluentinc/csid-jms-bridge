/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.exchange;

import io.confluent.amq.persistence.kafka.kcache.JournalCache;
import org.apache.activemq.artemis.api.core.QueueConfiguration;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.management.AddressControl;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.Binding;
import org.apache.activemq.artemis.core.postoffice.Bindings;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.Queue;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.management.ManagementService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import io.confluent.amq.ComponentLifeCycle.State;
import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic;
import io.confluent.amq.persistence.kafka.KafkaIO;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import io.confluent.amq.persistence.kafka.journal.KJournal;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaExchangeManagerTest {

  @Mock
  PostOffice mockPostOffice;

  @Mock
  KafkaIO mockKafkaIo;

  @Mock
  KafkaIntegration mockKafkaIntegration;

  @Mock
  ConfluentAmqServer mockAmqServer;

  @Mock
  StorageManager mockStorageManager;

  @Mock
  ManagementService mockManagementService;

  @Mock
  KafkaExchangeEgress mockEgress;

  @Mock
  KafkaExchangeIngress mockIngress;

  @Mock
  JournalCache mockBindings;

  @Mock
  JournalCache mockMessages;

  String messagesTopic = "messages";
  String bindingsTopic = "bindings";

  KafkaExchange kafkaExchange = new KafkaExchange();

  @BeforeEach
  public void setup() throws Exception {
    when(mockAmqServer.getKafkaIntegration()).thenReturn(mockKafkaIntegration);
    when(mockAmqServer.addOrUpdateAddressInfo(any(AddressInfo.class)))
        .thenAnswer(i -> i.getArgument(0));
    when(mockAmqServer.getManagementService()).thenReturn(mockManagementService);
    when(mockAmqServer.getPostOffice()).thenReturn(mockPostOffice);
    when(mockAmqServer.isActive()).thenReturn(true);
    when(mockAmqServer.getStorageManager()).thenReturn(mockStorageManager);

    when(mockAmqServer.updateQueue(any(QueueConfiguration.class))).thenReturn(mock(Queue.class));
    when(mockAmqServer.locateQueue(any(String.class))).thenReturn(mock(Queue.class));

    when(mockStorageManager.generateID()).thenReturn(1000L);

    when(mockKafkaIntegration.getKafkaIO()).thenReturn(mockKafkaIo);
    when(mockKafkaIntegration.getBindingsJournal()).thenReturn(mockBindings);
    when(mockKafkaIntegration.getMessagesJournal()).thenReturn(mockMessages);

    when(mockBindings.walTopic()).thenReturn(bindingsTopic);

    when(mockMessages.walTopic()).thenReturn(messagesTopic);
  }

  @Test
  @MockitoSettings(strictness = Strictness.LENIENT)
  public void missingRoutingConfigDisablesManager() throws Exception {
    //verify init does nothing
    //verify start does nothing
    //verify stop does nothing

    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    assertEquals(State.PREPARED, manager.getState());

    manager.init(Collections.emptyMap());
    assertEquals(State.PREPARED, manager.getState());

    manager.start();
    assertEquals(State.PREPARED, manager.getState());

    manager.stop();
    assertEquals(State.PREPARED, manager.getState());

    assertFalse(manager.isEnabled());
  }

  @Test
  @MockitoSettings(strictness = Strictness.LENIENT)
  public void initUpdatesState() {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder().buildPartial())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    manager.init(Collections.emptyMap());
    assertEquals(State.PREPARED, manager.getState());

  }

  @Test
  @MockitoSettings(strictness = Strictness.LENIENT)
  public void topicSyncRequiresStarting() throws Exception {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder().buildPartial())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    manager.init(Collections.emptyMap());
    manager.synchronizeTopics();
    verifyNoInteractions(mockAmqServer);
  }

  @Test
  public void discoveryOnlyDiscoversMatchingTopics() throws Exception {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder()
            .metadataRefreshMs(1000)
            .addTopics(new RoutedTopic.Builder()
                .match("foo.*"))
            .build())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    MockDiscoverySupport mockSupport = new MockDiscoverySupport();
    mockSupport.init();
    mockSupport.mockTopics(
        "foo-topic-1",
        "foobar-topic",
        "bar-foo-topic",
        "bar-topic",
        messagesTopic,
        bindingsTopic
    );

    manager.activated();
    Runnable runner = mockSupport.verifyAndCaptureExecution(1, 1000L);
    runner.run();

    assertEquals(2, kafkaExchange.getAllExchanges().size());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foo-topic-1")).count());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foobar-topic")).count());

  }

  @Test
  public void discoverySyncAddsNewMatches() throws Exception {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder()
            .addTopics(new RoutedTopic.Builder()
                .match("foo.*"))
            .build())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    manager.init(Collections.emptyMap());
    MockDiscoverySupport mockSupport = new MockDiscoverySupport();
    mockSupport.init();
    mockSupport.mockTopics(
        "foo-topic-1",
        "foobar-topic",
        "bar-foo-topic",
        "bar-topic",
        messagesTopic,
        bindingsTopic
    );

    manager.activated();
    Runnable runner = mockSupport.verifyAndCaptureExecution(1, 1000L);
    runner.run();

    assertEquals(2, kafkaExchange.getAllExchanges().size());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foo-topic-1")).count());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foobar-topic")).count());

    mockSupport.mockTopics(
        "foo-topic-1",
        "foo-topic-2",
        "foobar-topic",
        "bar-foo-topic",
        "bar-topic",
        messagesTopic,
        bindingsTopic
    );

    manager.synchronizeTopics();
    assertEquals(3, kafkaExchange.getAllExchanges().size());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foo-topic-2")).count());

  }

  @Test
  public void discoverySyncRemovesLostMatches() throws Exception {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder()
            .addTopics(new RoutedTopic.Builder()
                .match("foo.*"))
            .build())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    manager.init(Collections.emptyMap());
    MockDiscoverySupport mockSupport = new MockDiscoverySupport();
    mockSupport.init();
    mockSupport.mockTopics(
        "foo-topic-1",
        "foobar-topic",
        "bar-foo-topic",
        "bar-topic",
        messagesTopic,
        bindingsTopic
    );

    manager.activated();
    Runnable runner = mockSupport.verifyAndCaptureExecution(1, 1000L);
    runner.run();

    assertEquals(2, kafkaExchange.getAllExchanges().size());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foo-topic-1")).count());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foobar-topic")).count());

    mockSupport.mockTopics(
        "foobar-topic",
        "bar-foo-topic",
        "bar-topic",
        messagesTopic,
        bindingsTopic
    );
    Queue mockQueue = mock(Queue.class);
    String queueName = KafkaExchangeUtil.createIngressDestinationName("kafka.foo-topic-1");
    when(mockAmqServer.locateQueue(queueName)).thenReturn(mockQueue);

    manager.synchronizeTopics();
    assertEquals(1, kafkaExchange.getAllExchanges().size());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foobar-topic")).count());
    verify(mockQueue).deleteQueue(true);

  }

  @Test
  public void syncPausesBoundLostMatches() throws Exception {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder()
            .addTopics(new RoutedTopic.Builder()
                .match("foo.*"))
            .build())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    manager.init(Collections.emptyMap());
    MockDiscoverySupport mockSupport = new MockDiscoverySupport();
    mockSupport.init();
    mockSupport.mockTopics(
        "foo-topic-1",
        "foobar-topic"
    );

    manager.activated();
    Runnable runner = mockSupport.verifyAndCaptureExecution(1, 1000L);
    runner.run();


    mockSupport.mockTopics(
        "foobar-topic"
    );
    mockSupport.setNextBindingsCount(2);
    AddressControl mockAddressControl = mock(AddressControl.class);
    when(mockManagementService.getResource(matches("^.*foo-topic-1$")))
        .thenReturn(mockAddressControl);

    manager.synchronizeTopics();

    assertEquals(1, kafkaExchange.getAllExchanges().size());
    assertEquals(1, kafkaExchange.getAllExchanges().stream()
        .filter(e -> e.kafkaTopicName().equals("foobar-topic")).count());

    verify(mockAddressControl).pause(true);


  }

  @Test
  public void discoverySyncReschedules() throws Exception {
    BridgeConfig bridgeConfig = new BridgeConfig.Builder()
        .id("test")
        .routing(new RoutingConfig.Builder()
            .metadataRefreshMs(500)
            .addTopics(new RoutedTopic.Builder()
                .match("foo.*"))
            .build())
        .buildPartial();

    KafkaExchangeManager manager = new KafkaExchangeManager(bridgeConfig, mockKafkaIo);
    manager.prepare(mockAmqServer, kafkaExchange, mockIngress, mockEgress);

    manager.init(Collections.emptyMap());
    MockDiscoverySupport mockSupport = new MockDiscoverySupport();
    mockSupport.init();
    mockSupport.mockTopics(
        "foo-topic-1",
        "foobar-topic",
        "bar-foo-topic",
        "bar-topic",
        messagesTopic,
        bindingsTopic
    );

    manager.activated();
    Runnable runner = mockSupport.verifyAndCaptureExecution(1, 1000L);

    assertNotNull(runner);

    runner.run();
    runner = mockSupport.verifyAndCaptureExecution(1, 1000L);
    assertNotNull(runner);

  }

  class MockDiscoverySupport {

    final Queue mockQueue;
    final Bindings mockBindings;
    final Binding mockBinding;
    final ScheduledExecutorService mockExecutor;

    MockDiscoverySupport() {
      mockQueue = mock(Queue.class);
      mockBindings = mock(Bindings.class);
      mockBinding = mock(Binding.class);
      mockExecutor = mock(ScheduledExecutorService.class);
    }

    void init() throws Exception {
      when(mockAmqServer.getScheduledPool()).thenReturn(mock(ScheduledExecutorService.class));
      when(mockAmqServer.createQueue(any(QueueConfiguration.class))).thenReturn(mockQueue);
      when(mockPostOffice.getBindingsForAddress(any(SimpleString.class))).thenReturn(mockBindings);
      when(mockBindings.getBindings()).thenReturn(Arrays.asList(mockBinding));
      when(mockBinding.getUniqueName()).thenReturn(SimpleString.toSimpleString("mock-divert"));
      when(mockAmqServer.getScheduledPool()).thenReturn(mockExecutor);
    }

    void setNextBindingsCount(int count) {
      when(mockBindings.getBindings()).thenReturn(Collections.nCopies(count, mockBinding));
    }

    void mockTopics(String... topics) throws Exception {
      when(mockKafkaIo.listTopics()).thenReturn(new HashSet<>(Arrays.asList(topics)));
    }

    Runnable verifyAndCaptureExecution(int execCount, Long refreshTimeMs) {
      ArgumentCaptor<Runnable> runnerCapture = ArgumentCaptor.forClass(Runnable.class);
      verify(mockExecutor, times(execCount))
          .schedule(runnerCapture.capture(), eq(refreshTimeMs), eq(TimeUnit.MILLISECONDS));
      return runnerCapture.getValue();
    }
  }

}