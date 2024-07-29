/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import com.google.common.io.Resources;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import io.confluent.amq.config.BridgeClientId;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.BridgeConfigFactory;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.config.RoutingConfig.RoutedTopic.Builder;
import io.confluent.amq.test.AbstractContainerTest;
import io.confluent.amq.test.ArtemisTestServer;
import io.confluent.amq.test.ArtemisTestServer.Factory;
import io.confluent.amq.test.KafkaContainerHelper;
import io.confluent.amq.test.TestSupport;
import io.confluent.csid.common.utils.accelerator.Accelerator;
import io.confluent.csid.common.utils.accelerator.Owner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import javax.jms.*;
import javax.management.MBeanServer;
import javax.management.ObjectInstance;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
@SuppressFBWarnings({"MS_PKGPROTECT", "MS_SHOULD_BE_FINAL"})
@Tag("IntegrationTest")
public class JmsBridgeToKafkaTests extends AbstractContainerTest {

    public KafkaContainerHelper.AdminHelper adminHelper = getContainerHelper().adminHelper();

    public KafkaContainerHelper.ConsumerHelper<String, String> consumerHelper;

    public KafkaContainerHelper.ProducerHelper<String, String> producerHelper;

    public Properties bridgeKafkaProps;

    public BridgeConfig.Builder baseConfig = BridgeConfigFactory
            .loadConfiguration(Resources.getResource("base-test-config.conf"));

    @TempDir
    @Order(100)
    public static Path tempdir;

    @BeforeEach
    public void setup() throws Exception {
        consumerHelper = getContainerHelper().consumerHelper(
                TestSupport.stringDeserializer(),
                TestSupport.stringDeserializer());

        producerHelper = getContainerHelper().producerHelper(
                TestSupport.stringSerializer(),
                TestSupport.stringSerializer()
        );
        bridgeKafkaProps = new Properties();
        bridgeKafkaProps.setProperty(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getContainerHelper().bootstrapServers());
    }

    @Test
    public void testJmsPropertiesArePassedAlong() throws Exception {
        String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
        Factory amqf = ArtemisTestServer.factory();
        amqf.prepare(bridgeKafkaProps, b -> b
                .mutateJmsBridgeConfig(bridge -> bridge
                        .mergeFrom(baseConfig)
                        .id("test")
                        .routing(new RoutingConfig.Builder()
                                .addTopics(new Builder()
                                        .addressTemplate("test.${topic}")
                                        .match("customer-topic.*")
                                        .messageType("text"))
                                .build())));

        String customerAddress = "test." + kafkaCustomerTopic;

        try (
                ArtemisTestServer amq = amqf.start();
                Session session = amq.getConnection().createSession(true, Session.SESSION_TRANSACTED)
        ) {

            Topic topic = session.createTopic(customerAddress);
            consumerHelper.consumeBegin(kafkaCustomerTopic);

            try (MessageProducer producer = session.createProducer(topic)) {

                TextMessage message = session.createTextMessage("Message 1");
                message.setJMSReplyTo(topic);
                message.setStringProperty("foo", "bar");
                message.setJMSCorrelationID("FooCorrelationId");
                producer.send(message);
                session.commit();

                String topicString = "topic://" + topic.getTopicName();
                StringDeserializer strDeser = new StringDeserializer();
                LongDeserializer longDeser = new LongDeserializer();

                consumerHelper.lookUntil(record -> {

                    assertEquals(topicString,
                            strDeser
                                    .deserialize("", record.headers().lastHeader("jms.string.JMSReplyTo").value()));
                    assertNotNull(longDeser.deserialize("",
                            record.headers().lastHeader("jms.long.JMSMessageID").value()));
                    assertNotNull(
                            longDeser
                                    .deserialize("", record.headers().lastHeader("jms.long.JMSTimestamp").value()));
                    assertEquals("bar",
                            strDeser.deserialize("", record.headers().lastHeader("jms.string.foo").value()));
                    assertEquals("TEXT",
                            strDeser.deserialize("", record.headers().lastHeader("jms.string.JMSType").value()));
                    return true;
                });
            }
        }
    }

    @Test
    public void testSingleMessageForJmsAndKafka_OriginKafka() throws Exception {
        String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
        Factory amqf = ArtemisTestServer.factory();
        amqf.prepare(bridgeKafkaProps, b -> b
                .mutateJmsBridgeConfig(bridge -> bridge
                        .mergeFrom(baseConfig)
                        .id("test")
                        .routing(new RoutingConfig.Builder()
                                .addTopics(new Builder()
                                        .match("customer-topic.*")
                                        .messageType("text")
                                        .addressTemplate("test.${topic}"))

                                .build())));

        String customerAddress = "test." + kafkaCustomerTopic;
        String messagePayload = "Message Payload";

        try (
                ArtemisTestServer amq = amqf.start();
                Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
        ) {
            amq.assertAddressAvailable(customerAddress);

            Topic topic = session.createTopic(customerAddress);
            consumerHelper.consumeBegin(kafkaCustomerTopic);

            try (MessageConsumer consumer = session.createConsumer(topic)) {


                int attempts = 0;
                while (attempts < 10
                        && !amq.confluentAmqServer()
                        .getKafkaExchangeManager()
                        .currentSubscribedKafkaTopics()
                        .contains(kafkaCustomerTopic)) {

                    Thread.sleep(100);
                    attempts++;
                }

                producerHelper.publish(kafkaCustomerTopic, "key", messagePayload);


                ConsumerRecord<String, String> krecord = consumerHelper.lookUntil(record ->
                        messagePayload.equals(record.value()));
                assertNotNull(krecord);

                Message jmsMessage = consumer.receive(5000);

                assertNotNull(jmsMessage);
                assertEquals(messagePayload, jmsMessage.getBody(String.class));

            }


        }
    }

    @Test
    public void testSingleMessageForJmsAndKafka_OriginJms() throws Exception {
        String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
        consumerHelper.consumeBegin(kafkaCustomerTopic);

        Factory amqf = ArtemisTestServer.factory();
        amqf.prepare(bridgeKafkaProps, b -> b
                .mutateJmsBridgeConfig(bridge -> bridge
                        .mergeFrom(baseConfig)
                        .id("test")
                        .routing(new RoutingConfig.Builder()
                                .addTopics(new Builder()
                                        .match("customer-topic.*")
                                        .messageType("text")
                                        .addressTemplate("test.${topic}"))

                                .build())));

        String customerAddress = "test." + kafkaCustomerTopic;
        String messagePayload = "Message Payload";

        try (
                ArtemisTestServer amq = amqf.start();
                Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
        ) {
            amq.assertAddressAvailable(customerAddress);

            Topic topic = session.createTopic(customerAddress);

            try (MessageProducer producer = session.createProducer(topic);
                 MessageConsumer consumer = session.createConsumer(topic)) {

                TextMessage message = session.createTextMessage(messagePayload);
                producer.send(message);

                ConsumerRecord<String, String> krecord = consumerHelper.lookUntil(m -> true, Duration.ofSeconds(5));

                assertNotNull(krecord);
                assertEquals(messagePayload, krecord.value());

                Message jmsMessage = consumer.receive(1000);
                Message jmsMessage2 = consumer.receive(1000);

                assertNotNull(jmsMessage);
                assertNull(jmsMessage2);
                assertEquals(messagePayload, jmsMessage.getBody(String.class));

                producerHelper.publish(kafkaCustomerTopic, "key", messagePayload);
                jmsMessage = consumer.receive(1000);
                assertNotNull(jmsMessage);
                assertEquals(messagePayload, jmsMessage.getBody(String.class));

            }


        }
    }

    @Test
    public void testKeySelection() throws Exception {
        String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
        Factory amqf = ArtemisTestServer.factory();
        amqf.prepare(bridgeKafkaProps, b -> b
                .mutateJmsBridgeConfig(bridge -> bridge
                        .mergeFrom(baseConfig)
                        .id("test")
                        .routing(new RoutingConfig.Builder()
                                .putAllProducer(baseConfig.kafka())
                                .addTopics(new Builder()
                                        .match("customer-topic.*")
                                        .messageType("text")
                                        .addressTemplate("test.${topic}"))

                                .build())));

        String customerAddress = "test." + kafkaCustomerTopic;

        try (
                ArtemisTestServer amq = amqf.start();
                Session session = amq.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
        ) {
            amq.assertAddressAvailable(customerAddress);

            Topic topic = session.createTopic(customerAddress);
            consumerHelper.consumeBegin(kafkaCustomerTopic);

            try (MessageProducer producer = session.createProducer(topic)) {

                TextMessage message = session.createTextMessage("Message 1");
                message.setJMSCorrelationID("FooCorrelationId");
                producer.send(message);
                ConsumerRecord<String, String> kafkaRecord =
                        consumerHelper.lookUntil(r -> "FooCorrelationId".equals(r.key()));
                assertNotNull(kafkaRecord);

            }
        }
    }

    @Test
    public void testKafkaClientIdsAreCorrect() throws Exception {

        String kafkaCustomerTopic = adminHelper.safeCreateTopic("customer-topic", 1);
        Factory amqf = ArtemisTestServer.factory();
        String bridgeId = "my_client_test_bridge";
        String sfcdId = "my_partner_sfdc_id";
        String clientIdPrefix =
                String.format("BWC | %s | 01956412-5721-46ba-9673-4a84b48200b2 | unknown | %s", sfcdId, bridgeId);
        BridgeConfig.Builder idConfig = BridgeConfigFactory
                .loadConfiguration(Resources.getResource("config/partner-sfdc-id-test-config.conf"));

        amqf.prepare(bridgeKafkaProps, b -> b
                .mutateJmsBridgeConfig(bridge -> bridge
                        .mergeFrom(idConfig)
                        .routing(new RoutingConfig.Builder()
                                .addTopics(new Builder()
                                        .addressTemplate("test.${topic}")
                                        .match("customer-topic.*")
                                        .messageType("text"))
                                .build())));

        String customerAddress = "test." + kafkaCustomerTopic;

        try (
                ArtemisTestServer amq = amqf.start();
                Session session = amq.getConnection().createSession(true, Session.SESSION_TRANSACTED)
        ) {

            Topic topic = session.createTopic(customerAddress);

            try (MessageProducer producer = session.createProducer(topic)) {

                TextMessage message = session.createTextMessage("Message 1");
                message.setJMSReplyTo(topic);
                message.setStringProperty("foo", "bar");
                message.setJMSCorrelationID("FooCorrelationId");
                producer.send(message);
                session.commit();

                //now publish to kafka to trigger the exchange consumer
                producerHelper.publish(kafkaCustomerTopic, "key", "Message 2");

                MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
                Set<ObjectInstance> objectInstanceSet = mBeanServer.queryMBeans(null, null);
                Long bridgeCount = objectInstanceSet.stream()
                        .map(ObjectInstance::toString)
                        .filter(s -> s.contains("client-id=\"" + bridgeId))
                        .peek(System.err::println)
                        .count();

                Long properIdCount = mBeanServer.queryMBeans(null, null).stream()
                        .map(ObjectInstance::toString)
                        .filter(s -> s.contains("client-id"))
                        .filter(s -> s.contains(clientIdPrefix))
                        .count();


                assertEquals(
                        0, bridgeCount, "one or more bridge client ids do not follow the correct pattern");
                assertTrue(properIdCount > 0);
            }
        }
    }
}

