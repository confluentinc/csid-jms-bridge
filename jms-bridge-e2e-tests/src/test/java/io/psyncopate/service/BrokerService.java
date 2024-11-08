package io.psyncopate.service;

import io.psyncopate.server.ServerSetup;
import io.psyncopate.util.constants.*;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;

import javax.jms.JMSException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

@RequiredArgsConstructor
public class BrokerService {
    private final ServerSetup serverSetup;

    private HashMap<String, String> getKafkaHostPort() {
        HashMap<String, String> server = new HashMap<>();
        server.put(Constants.HOST, serverSetup.getConfigLoader().getKafkaHost());
        server.put(Constants.APP_PORT, serverSetup.getConfigLoader().getKafkaPort());
        return server;
    }

    public int startProducer(ServerType serverType, MessagingScheme messagingScheme, String address, int messageToBeSent) throws JMSException {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            return serverSetup.startJmsProducer(serverType, address, messagingScheme.getRoutingType(), messageToBeSent);
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return serverSetup.startKafkaProducer(getKafkaHostPort(), address, messageToBeSent);
        }
        return -1;
    }

    public void ensureKafkaTopicExists(String topic) {
        Map<String, String> server = getKafkaHostPort();
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server.get(Constants.HOST) + ":" + server.get(Constants.APP_PORT));
        try (AdminClient adminClient = AdminClient.create(properties)) {
            boolean topicExists = false;
            while (!topicExists) {
                try {
                    topicExists = adminClient.describeTopics(Collections.singleton(topic)).allTopicNames().get().containsKey(topic);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    if (e.getCause() == null || !(e.getCause() instanceof UnknownTopicOrPartitionException)) {
                        throw new RuntimeException(e);
                    }
                }
                try {
                    if (!topicExists) {
                        adminClient.createTopics(Collections.singleton(new NewTopic(topic, 1, (short) 1))).all().get();
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (ExecutionException e) {
                    throw new RuntimeException(e);
                }
            }
        }


    }

    public int startProducer(ServerType serverType, MessagingScheme messagingScheme, String address) throws JMSException {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            return serverSetup.startJmsProducer(serverType, address, messagingScheme.getRoutingType(), -1);
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {

            return serverSetup.startKafkaProducer(getKafkaHostPort(), address, -1);
        }
        return -1;
    }

    public CompletableFuture<Integer> startAsyncKafkaProducer(String address, AtomicBoolean stopFlag) {
        return serverSetup.startKafkaProducerAsync(getKafkaHostPort(), address, -1, stopFlag);
    }

    public CompletableFuture<Integer> startAsyncJmsProducer(ServerType serverType, MessagingScheme messagingScheme, String address) {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            return serverSetup.startJmsProducerAsync(serverType, address, messagingScheme.getRoutingType());
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            throw new IllegalArgumentException("Kafka serverType should not be used with startAsyncJmsProducer");
        }
        return CompletableFuture.completedFuture(-1);
    }


    public CompletableFuture<Integer> startAsyncConsumer(ServerType serverType, MessagingScheme messagingScheme, String... address) {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            String destination = address[0];
            if (RoutingType.MULTICAST == messagingScheme.getRoutingType()) {
                destination = address[0] + "::" + address[1];
            }
            return serverSetup.startJmsConsumerAsync(serverType, destination, messagingScheme.getRoutingType(), 400L);
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return CompletableFuture.completedFuture(0);
        }
        return CompletableFuture.completedFuture(-1);
    }

    public int startConsumer(ServerType serverType, MessagingScheme messagingScheme, String... address) throws JMSException, InterruptedException {
        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            String destination = address[0];
            if (RoutingType.MULTICAST == messagingScheme.getRoutingType()) {
                destination = address[0] + "::" + address[1];
            }
            //TODO need to pass message count
            return serverSetup.startJmsConsumer(serverType, destination, messagingScheme.getRoutingType());
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return serverSetup.startKafkaConsumer(getKafkaHostPort(), address[0], -1);
        }
        return -1;
    }

    public int startConsumer(ServerType serverType, MessagingScheme messagingScheme, int messageCountToBeConsumed, String... address) throws JMSException, InterruptedException {
        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            String destination = address[0];
            if (RoutingType.MULTICAST == messagingScheme.getRoutingType()) {
                destination = address[0] + "::" + address[1];
            }
            //TODO need to pass message count
            return serverSetup.startJmsConsumer(serverType, destination, messagingScheme.getRoutingType());
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return serverSetup.startKafkaConsumer(getKafkaHostPort(), address[0], messageCountToBeConsumed);
        }
        return -1;
    }
}
