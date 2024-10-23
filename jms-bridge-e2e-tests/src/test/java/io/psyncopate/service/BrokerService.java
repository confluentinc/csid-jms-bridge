package io.psyncopate.service;

import io.psyncopate.server.ServerSetup;
import io.psyncopate.util.constants.BrokerType;
import io.psyncopate.util.constants.Constants;
import io.psyncopate.util.constants.MessagingScheme;
import io.psyncopate.util.constants.RoutingType;
import lombok.RequiredArgsConstructor;

import javax.jms.JMSException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;

@RequiredArgsConstructor
public class BrokerService {
    private final ServerSetup serverSetup;

    private HashMap<String, String> getKafkaHostPort() {
        HashMap<String, String> server = new HashMap<>();
        server.put(Constants.HOST, serverSetup.getConfigLoader().getKafkaHost());
        server.put(Constants.APP_PORT, serverSetup.getConfigLoader().getKafkaPort());
        return server;
    }

    public int startProducer(MessagingScheme messagingScheme, String address, int messageToBeSent) throws JMSException {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            return serverSetup.startJmsProducer(address, messagingScheme.getRoutingType(), messageToBeSent);
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return serverSetup.startKafkaProducer(getKafkaHostPort(), address, messageToBeSent);
        }
        return -1;
    }

    public int startProducer(MessagingScheme messagingScheme, String address) throws JMSException {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            return serverSetup.startJmsProducer(address, messagingScheme.getRoutingType(), -1);
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {

            return serverSetup.startKafkaProducer(getKafkaHostPort(), address, -1);
        }
        return -1;
    }


    public CompletableFuture<Integer> startAsyncProducer(MessagingScheme messagingScheme, String address) throws JMSException {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            return serverSetup.startJmsProducerAsync(address, messagingScheme.getRoutingType());
        }
//        else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
//            return CompletableFuture.completedFuture(0);
//        }
        return CompletableFuture.completedFuture(-1);
    }


    public CompletableFuture<Integer> startAsyncConsumer(MessagingScheme messagingScheme, String... address) throws JMSException {

        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            String destination = address[0];
            if (RoutingType.MULTICAST == messagingScheme.getRoutingType()) {
                destination = address[0] + "::" + address[1];
            }
            return serverSetup.startJmsConsumerAsync(destination, messagingScheme.getRoutingType(), 400L);
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return CompletableFuture.completedFuture(0);
        }
        return CompletableFuture.completedFuture(-1);
    }

    public int startConsumer(MessagingScheme messagingScheme, String... address) throws JMSException, InterruptedException {
        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            String destination = address[0];
            if (RoutingType.MULTICAST == messagingScheme.getRoutingType()) {
                destination = address[0] + "::" + address[1];
            }
            //TODO need to pass message count
            return serverSetup.startJmsConsumer(destination, messagingScheme.getRoutingType());
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return serverSetup.startKafkaConsumer(getKafkaHostPort(), address[0], -1);
        }
        return -1;
    }

    public int startConsumer(MessagingScheme messagingScheme, int messageCountToBeConsumed, String... address) throws JMSException, InterruptedException {
        if (BrokerType.JMS == messagingScheme.getBrokerType()) {
            String destination = address[0];
            if (RoutingType.MULTICAST == messagingScheme.getRoutingType()) {
                destination = address[0] + "::" + address[1];
            }
            //TODO need to pass message count
            return serverSetup.startJmsConsumer(destination, messagingScheme.getRoutingType());
        } else if (BrokerType.KAFKA == messagingScheme.getBrokerType()) {
            return serverSetup.startKafkaConsumer(getKafkaHostPort(), address[0], messageCountToBeConsumed);
        }
        return -1;
    }
}
