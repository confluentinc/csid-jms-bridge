package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import lombok.EqualsAndHashCode;
import lombok.Value;

import java.util.Date;

@Value
public class TimestampedJournalKey {
    JournalEntryKey entry;

    @EqualsAndHashCode.Exclude
    Date timestamp;

}
