package io.confluent.amq.persistence.kafka.journal.serde;

import io.confluent.amq.persistence.domain.proto.JournalEntryRefKey;
import lombok.Builder;
import lombok.Data;
import lombok.experimental.FieldDefaults;

@Data
@Builder(toBuilder = true)
@FieldDefaults(level = lombok.AccessLevel.PRIVATE)
public class JournalEntryKey {
    long messageId;
    long txId;
    int extendedId;

    public static JournalEntryKey fromRefKey(JournalEntryRefKey key) {
        if (key == null) {
            return null;
        }
        return JournalEntryKey.builder()
                .messageId(key.getMessageId())
                .txId(key.getTxId())
                .extendedId(key.getExtendedId())
                .build();
    }

    public boolean isEmpty() {
        return (messageId == 0 && txId == 0 && extendedId == 0);
    }

    public JournalEntryRefKey toRefKey() {
        return JournalEntryRefKey.newBuilder()
                .setMessageId(messageId)
                .setTxId(txId)
                .setExtendedId(extendedId)
                .build();
    }
}
