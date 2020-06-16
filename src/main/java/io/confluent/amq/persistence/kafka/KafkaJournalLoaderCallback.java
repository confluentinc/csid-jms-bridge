package io.confluent.amq.persistence.kafka;

import org.apache.activemq.artemis.core.journal.LoaderCallback;
import org.apache.activemq.artemis.core.journal.PreparedTransactionInfo;
import org.apache.activemq.artemis.core.journal.RecordInfo;

import java.util.List;

public class KafkaJournalLoaderCallback implements LoaderCallback {
    @Override
    public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction) {

    }

    @Override
    public void addRecord(RecordInfo info) {

    }

    @Override
    public void deleteRecord(long id) {

    }

    @Override
    public void updateRecord(RecordInfo info) {

    }

    @Override
    public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete) {

    }
}
