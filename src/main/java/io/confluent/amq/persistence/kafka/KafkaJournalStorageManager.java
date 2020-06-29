package io.confluent.amq.persistence.kafka;

import io.confluent.amq.KafkaBridgePlugin;
import org.apache.activemq.artemis.api.core.*;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.core.config.storage.DatabaseStorageConfiguration;
import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.journal.JournalLoadInformation;
import org.apache.activemq.artemis.core.message.impl.CoreMessage;
import org.apache.activemq.artemis.core.paging.PagingManager;
import org.apache.activemq.artemis.core.persistence.AddressBindingInfo;
import org.apache.activemq.artemis.core.persistence.CoreMessageObjectPools;
import org.apache.activemq.artemis.core.persistence.GroupingInfo;
import org.apache.activemq.artemis.core.persistence.QueueBindingInfo;
import org.apache.activemq.artemis.core.persistence.impl.PageCountPending;
import org.apache.activemq.artemis.core.persistence.impl.journal.JDBCJournalStorageManager;
import org.apache.activemq.artemis.core.persistence.impl.journal.JournalStorageManager;
import org.apache.activemq.artemis.core.postoffice.PostOffice;
import org.apache.activemq.artemis.core.server.impl.JournalLoader;
import org.apache.activemq.artemis.core.transaction.ResourceManager;
import org.apache.activemq.artemis.jdbc.store.drivers.JDBCUtils;
import org.apache.activemq.artemis.jdbc.store.file.JDBCSequentialFileFactory;
import org.apache.activemq.artemis.jdbc.store.journal.JDBCJournalImpl;
import org.apache.activemq.artemis.jdbc.store.sql.PropertySQLProvider;
import org.apache.activemq.artemis.jdbc.store.sql.SQLProvider;
import org.apache.activemq.artemis.reader.BytesMessageUtil;
import org.apache.activemq.artemis.reader.TextMessageUtil;
import org.apache.activemq.artemis.spi.core.protocol.MessagePersister;
import org.apache.activemq.artemis.utils.ExecutorFactory;
import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.commons.io.Charsets;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jboss.logging.Logger;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class KafkaJournalStorageManager extends JournalStorageManager {
    private static final Logger logger = Logger.getLogger(KafkaJournalStorageManager.class);

    private Properties kafkaProps;
    private KafkaIO kafkaIO;
    private StringSerializer stringSerializer = new StringSerializer();

    public KafkaJournalStorageManager(Configuration config, CriticalAnalyzer analyzer, ExecutorFactory executorFactory, ScheduledExecutorService scheduledExecutorService, ExecutorFactory ioExecutors) {
        super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutors);
    }

    public KafkaJournalStorageManager(Configuration config, CriticalAnalyzer analyzer, ExecutorFactory executorFactory, ExecutorFactory ioExecutors) {
        super(config, analyzer, executorFactory, ioExecutors);
    }

    public KafkaJournalStorageManager(Configuration config, CriticalAnalyzer analyzer, ExecutorFactory executorFactory, ScheduledExecutorService scheduledExecutorService, ExecutorFactory ioExecutors, IOCriticalErrorListener criticalErrorListener) {
        super(config, analyzer, executorFactory, scheduledExecutorService, ioExecutors, criticalErrorListener);
    }

    public KafkaJournalStorageManager(Configuration config, CriticalAnalyzer analyzer, ExecutorFactory executorFactory, ExecutorFactory ioExecutors, IOCriticalErrorListener criticalErrorListener) {
        super(config, analyzer, executorFactory, ioExecutors, criticalErrorListener);
    }

    public void setKafkaProps(Properties kafkaProps) {
        this.kafkaProps = kafkaProps;
    }

    @Override
    public synchronized void start() throws Exception {
        super.start();
        kafkaIO = new KafkaIO(kafkaProps);
        kafkaIO.start();
    }


    @Override
    public void stop() throws Exception {
        super.stop();
        if(kafkaIO != null) {
            kafkaIO.stop();
        }
    }

    @Override
    public void storeMessage(Message message) throws Exception {
        ICoreMessage coreMessage =  message.toCore();
        if(! KafkaIO.isKafkaMessage(coreMessage)) {
            message.toCore().setAnnotation(SimpleString.toSimpleString("KAFKA_REF"), "TBD");
            KafkaIO.KafkaRef kafkaRef = kafkaIO.writeMessage(message).get();
            message.toCore().setAnnotation(SimpleString.toSimpleString("KAFKA_REF"), kafkaRef.asString());
        }
        super.storeMessage(coreMessage);
    }


    @Override
    public JournalLoadInformation loadMessageJournal(PostOffice postOffice, PagingManager pagingManager, ResourceManager resourceManager, Map<Long, QueueBindingInfo> queueInfos, Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap, Set<Pair<Long, Long>> pendingLargeMessages, List<PageCountPending> pendingNonTXPageCounter, JournalLoader journalLoader) throws Exception {
        JournalLoadInformation loadInfo =  super.loadMessageJournal(postOffice, pagingManager, resourceManager, queueInfos, duplicateIDMap, pendingLargeMessages, pendingNonTXPageCounter, journalLoader);

        logger.info("###### LoadMessageJournal completed.");
        String delim = "    " + System.lineSeparator();
        String addresses = postOffice.getAddresses().stream().map(SimpleString::toString).collect(Collectors.joining(delim));
        logger.info("###### Known Addresses: " + delim + addresses);

        String queues = queueInfos.values().stream().map(qb -> qb.getAddress() + "::" + qb.getQueueName()).collect(Collectors.joining(delim));
        logger.info("###### Known Queues: " + delim + queues);

        return loadInfo;
    }

    @Override
    public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos, List<GroupingInfo> groupingInfos, List<AddressBindingInfo> addressBindingInfos) throws Exception {
        return super.loadBindingJournal(queueBindingInfos, groupingInfos, addressBindingInfos);
    }

    @Override
    public JournalLoadInformation[] loadInternalOnly() throws Exception {
        return super.loadInternalOnly();
    }


}
