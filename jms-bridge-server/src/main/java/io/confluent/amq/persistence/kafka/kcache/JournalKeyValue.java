package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import lombok.Value;

@Value
public class JournalKeyValue {
    JournalEntryKey key;
    JournalEntry value;
}
