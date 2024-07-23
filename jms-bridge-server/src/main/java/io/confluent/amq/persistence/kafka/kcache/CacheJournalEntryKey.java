package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.persistence.domain.proto.JournalEntryKey;

public class CacheJournalEntryKey implements Comparable<CacheJournalEntryKey>{

    private final JournalEntryKey journalEntryKey;

    public CacheJournalEntryKey(JournalEntryKey journalEntryKey) {
        this.journalEntryKey = journalEntryKey;
    }

    public JournalEntryKey getJournalEntryKey() {
        return journalEntryKey;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CacheJournalEntryKey) {
            return journalEntryKey.equals(((CacheJournalEntryKey) obj).journalEntryKey);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return journalEntryKey.hashCode();
    }

    @Override
    public int compareTo(CacheJournalEntryKey o) {
        if(o == null || o.getJournalEntryKey() == null) {
            return 1;
        }

        return Long.compare(journalEntryKey.getMessageId(), o.journalEntryKey.getMessageId());
    }
}
