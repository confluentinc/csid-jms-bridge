package io.confluent.amq.cli;

import com.google.common.io.Resources;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.test.AbstractContainerTest;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.TestSupport;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import java.nio.file.Path;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(CapturedCommandIo.class)
public class ReceiveCommandTest extends AbstractContainerTest {

  public Properties bridgeKafkaProps;

  public BridgeConfig.Builder baseConfig = BridgeConfigFactory
      .loadConfiguration(Resources.getResource("base-test-config.conf"));

  @BeforeEach
  public void setup() throws Exception {
    bridgeKafkaProps = new Properties();
    bridgeKafkaProps.setProperty(
        ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getContainerHelper().bootstrapServers());
  }

  @Test
  @Disabled("IO is unpredictable")
  public void testReceive(CapturedCommandIo commandIo) throws Exception {
    ArtemisTestServer.Factory amqf = ArtemisTestServer.factory();
    amqf.prepare(bridgeKafkaProps, b -> b
        .mutateJmsBridgeConfig(bridge -> bridge
            .mergeFrom(baseConfig)
            .id("test")
            .build()));

    String topicName = "foo";
    String messagePayload = "Message Payload";

    try (
        ArtemisTestServer amq = amqf.start();
        Session session = amq.getConnection().createSession(true, Session.SESSION_TRANSACTED)
    ) {

      Topic topic = session.createTopic(topicName);
      try (MessageProducer producer = session.createProducer(topic)) {

        JmsClientOptions jmsOptions = new JmsClientOptions();
        jmsOptions.brokerUrl = amq.url();

        ReceiveCommand receiveCommand = new ReceiveCommand();
        receiveCommand.jmsClientOptions = jmsOptions;
        receiveCommand.topic = topicName;
        CompletableFuture<Void> receiveFuture = null;

        try {
          receiveFuture = CompletableFuture.runAsync(
              TestSupport.runner(() -> receiveCommand.execute(commandIo)));

          //purge the outputs
          commandIo.readAllOutputLines();

          producer.send(session.createTextMessage(messagePayload));
          TestSupport.retry(5, 50, () -> {
            String receivedPayload = commandIo.outputReader().readLine();
            assertEquals(messagePayload, receivedPayload);
          });

        } finally {
          if (receiveFuture != null) {
            receiveFuture.cancel(true);
          }
        }
      }
    }
  }
}

