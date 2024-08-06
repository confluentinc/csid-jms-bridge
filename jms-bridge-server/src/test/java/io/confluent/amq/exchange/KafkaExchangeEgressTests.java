package io.confluent.amq.exchange;

import io.confluent.amq.ConfluentAmqServer;
import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.config.RoutingConfig;
import io.confluent.amq.persistence.kafka.KafkaIO;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.persistence.StorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.postoffice.RoutingStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.Optional;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith({MockitoExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
public class KafkaExchangeEgressTests {
    @Mock
    KafkaIO mockKafkaIo;

    @Mock
    KafkaExchange mockKafkaExchange;

    @Mock
    ConfluentAmqServer mockAmqServer;

    @Mock
    StorageManager mockStorageManager;
    @Mock
    PostOffice mockPostOffice;

    @Test
    public void testTombstone() throws Exception {
       BridgeConfig config = new BridgeConfig.Builder()
               .id("test-bridge-id")
               .buildPartial();
       KafkaExchangeEgress egress = new KafkaExchangeEgress(config, mockAmqServer, mockKafkaExchange,mockKafkaIo);
       ConsumerRecord<byte[], byte[]> record = new ConsumerRecord<>(
                "topic", 0, 0L, "key".getBytes(UTF_8), null);


       KafkaTopicExchange kte = new KafkaTopicExchange.Builder()
               .amqAddressName("amqAddressName")
               .originConfig(new RoutingConfig
                       .RoutedTopic.Builder()
                       .messageType(KExMessageType.BYTES.name())
                       .buildPartial())
               .buildPartial();
       when(mockKafkaExchange.findByTopic("topic")).thenReturn(Optional.of(kte));

       when(mockAmqServer.getStorageManager()).thenReturn(mockStorageManager);
       when(mockStorageManager.generateID()).thenReturn(1L);

       when(mockAmqServer.getPostOffice()).thenReturn(mockPostOffice);
       when(mockPostOffice.route(any(CoreMessage.class), anyBoolean())).thenReturn(RoutingStatus.OK);

       egress.onRecieve(record);
       verify(mockPostOffice, times(1)).route(any(CoreMessage.class), anyBoolean());
    }


    /*
        CoreMessage coreMessage = new CoreMessage(
        server.getStorageManager().generateID(), kafkaRecord.serializedKeySize() + 50);

        RoutingStatus status = server.getPostOffice().route(coreMessage, false);


        Optional<KafkaTopicExchange> exchangeOpt = kafkaExchange.findByTopic(kafkaRecord.topic());

        //can skip this by not calling doStart()
            consumerThread = kafkaIO.startConsumerThread(b -> b
        .addAllTopics(kafkaExchange.allExchangeKafkaTopics())
        .groupId(config.id() + ".exchange")
        .receiver(this)
        .pollMs(100L)
        .valueDeser(new ByteArrayDeserializer())
        .keyDeser(new ByteArrayDeserializer()));
     */
}
