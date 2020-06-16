package io.confluent.amq;

import io.confluent.amq.persistence.kafka.KafkaIO;
import org.apache.activemq.artemis.api.core.ActiveMQBuffer;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.Message;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.activemq.artemis.core.server.ActiveMQServer;
import org.apache.activemq.artemis.core.server.RoutingContext;
import org.apache.activemq.artemis.core.server.ServerSession;
import org.apache.activemq.artemis.core.server.impl.AddressInfo;
import org.apache.activemq.artemis.core.server.plugin.ActiveMQServerPlugin;
import org.apache.activemq.artemis.core.transaction.Transaction;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.utils.Utils;
import org.jboss.logging.Logger;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class KafkaBridgePlugin implements ActiveMQServerPlugin, KafkaIO.MessageAdapter {
    private static final Logger logger = Logger.getLogger(KafkaBridgePlugin.class);
    private Properties kafkaProps;
    private KafkaIO kafkaIO;
    private volatile ActiveMQServer server;

    public KafkaBridgePlugin(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    @Override
    public void registered(ActiveMQServer server) {
        logger.info("###### Registered called");

        this.kafkaIO = new KafkaIO(this.kafkaProps);
        this.kafkaIO.start();

        this.server = server;
    }

    @Override
    public void init(Map<String, String> properties) {
        logger.info("###### Init Called");

    }

    @Override
    public void unregistered(ActiveMQServer server) {
        logger.info("###### unregistered called");
        if(this.kafkaIO != null) {
            try {
                this.kafkaIO.stop();
            } catch(Exception e) {
                logger.warn("Error occurred while attempting to stop Kafka bridge during unregistration.", e);
            }
        }
    }

    @Override
    public void receive(Message message) {
        try {
            logger.info("Received message from kafka topic: " + message);
            server.getPostOffice().route(message, false);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Message transform(ConsumerRecord<byte[], byte[]> kafkaRecord) {

        ByteBuffer buffer = ByteBuffer.allocate(8);
        buffer.putInt((int)kafkaRecord.offset());
        buffer.putShort((short) kafkaRecord.partition());
        int hashedTopic = Utils.murmur2(kafkaRecord.topic().getBytes());
        buffer.putShort((short) hashedTopic);
        buffer.flip();
        long id = buffer.getLong();


        CoreMessage message = new CoreMessage(id, kafkaRecord.value().length);
        BytesMessageUtil.bytesWriteBytes(message.getBodyBuffer(), kafkaRecord.value());
        message.setDurable(true);
        return message;
    }

    private void updateKafkaSubscriptions() {
        Collection<String> knownTopics = kafkaIO.getKnownTopics();
        List<String> subTopics = server.getPostOffice().getAddresses().stream()
                .map(SimpleString::toString)
                .filter(knownTopics::contains)
                .collect(Collectors.toList());
        if(subTopics.size() > 0) {
            logger.info("###### Updating kafka topics being consumed to: " + String.join(", ", subTopics));
            kafkaIO.readTopics(subTopics, this);
        }
    }

    @Override
    public void afterAddAddress(AddressInfo addressInfo, boolean reload) throws ActiveMQException {
        kafkaIO.refreshTopics();
        updateKafkaSubscriptions();
    }

    @Override
    public void afterRemoveAddress(SimpleString address, AddressInfo addressInfo) throws ActiveMQException {
        updateKafkaSubscriptions();
    }

    private void logTextMessage(String prefix, Message message) {
        if(message.toCore().getType() == Message.TEXT_TYPE) {
            logger.info("##### " + prefix + ": " + TextMessageUtil.readBodyText(message.toCore().getReadOnlyBodyBuffer()));
        }
    }

    @Override
    public void afterSend(ServerSession session, Transaction tx, Message message, boolean direct, boolean noAutoCreateQueue, RoutingStatus result) throws ActiveMQException {
        logTextMessage("after send", message);
    }

    @Override
    public void afterMessageRoute(Message message, RoutingContext context, boolean direct, boolean rejectDuplicates, RoutingStatus result) throws ActiveMQException {
        logTextMessage("after message route", message);
    }
}
