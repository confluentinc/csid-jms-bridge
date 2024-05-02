package io.confluent.amq.ha;

import com.google.common.io.Resources;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.test.AbstractContainerTest;
import io.confluent.amq.test.ArtemisTestServer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.*;
import javax.jms.IllegalStateException;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BasicHaTest extends AbstractContainerTest {
    static final String backupBrokerXml = "brokerxml/broker-backup-ha.xml";
    static final String liveBrokerXml = "brokerxml/broker-live-ha.xml";

    //start amq servers on different ports
    @TempDir
    @Order(100)
    public static Path tempdirLive;

    @TempDir
    @Order(100)
    public static Path tempdirBackup;



    @RegisterExtension
    @Order(250)
    public static final ArtemisTestServer backupServer = ArtemisTestServer
            .embedded(essentialProps(), b -> b
                    .isBackup(true)
                    .brokerXml("file://" + Resources.getResource(backupBrokerXml).getPath())
                    .mutateJmsBridgeConfig(br -> br
                            .putKafka(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                            .putKafka(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
                            .routing(new RoutingConfig.Builder()
                                    .addTopics(new RoutingConfig.RoutedTopic.Builder()
                                            .messageType("TEXT")
                                            .match("response.*")
                                            .consumeAlways(true)
                                            .addressTemplate("test.${topic}"))
                                    .addTopics(new RoutingConfig.RoutedTopic.Builder()
                                            .messageType("TEXT")
                                            .match("request.*")
                                            .addressTemplate("test.${topic}"))
                                    .build())
                    )
                    .dataDirectory(tempdirBackup.toAbsolutePath().toString()),
                    b -> b.url("(tcp://localhost:61615?name=node-0,tcp://localhost:61618?name=node-1)?ha=true&reconnectAttempts=-1"));

    @RegisterExtension
    @Order(300)
    public static final ArtemisTestServer liveServer = ArtemisTestServer
            .embedded(essentialProps(), b -> b
                    .brokerXml("file://" + Resources.getResource(liveBrokerXml).getPath())
                    .mutateJmsBridgeConfig(br -> br
                            .putKafka(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
                            .putKafka(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "500")
                            .routing(new RoutingConfig.Builder()
                                    .addTopics(new RoutingConfig.RoutedTopic.Builder()
                                            .messageType("TEXT")
                                            .match("response.*")
                                            .consumeAlways(true)
                                            .addressTemplate("test.${topic}"))
                                    .addTopics(new RoutingConfig.RoutedTopic.Builder()
                                            .messageType("TEXT")
                                            .match("request.*")
                                            .addressTemplate("test.${topic}"))
                                    .build())
                    )
                    .dataDirectory(tempdirLive.toAbsolutePath().toString()),
                    b -> b.url("(tcp://localhost:61615?name=node-0,tcp://localhost:61618?name=node-1)?ha=true&reconnectAttempts=-1"));


    @Test
    public void testScenario() throws Exception {
        String topicName = "foo";
        String consumerName = "testScenario";
        String messageBody = "testing 1 2 3";
        String messageBody2 = "testing 3 2 1";
        try (
                Session session = liveServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
        ) {
            Topic topic = session.createTopic(topicName);

            try (
                    MessageConsumer consumer = session.createDurableConsumer(topic, consumerName)
            ) {

                try (MessageProducer producer = session.createProducer(topic)) {

                    producer.send(session.createTextMessage(messageBody));

                    Message msg = consumer.receive(5000L);

                    assertEquals(messageBody, msg.getBody(String.class));

                    producer.send(session.createTextMessage(messageBody2));

                    //kill live server
                    liveServer.stop();

                    try {
                        msg = consumer.receive(5000L);
                    } catch (IllegalStateException e) {

                    }


                    assertEquals(messageBody, msg.getBody(String.class));

                }
            }
        }
    }
}