package io.confluent.amq.persistence.kafka.kcache;

import io.confluent.amq.persistence.domain.proto.JournalEntry;

public class CacheJournalEntry implements Comparable<CacheJournalEntry> {

    private final JournalEntry journalEntry;

    public CacheJournalEntry(JournalEntry journalEntry) {
        this.journalEntry = journalEntry;
    }

    public JournalEntry getJournalEntry() {
        return journalEntry;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof CacheJournalEntry) {
            return journalEntry.equals(((CacheJournalEntry) obj).journalEntry);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return journalEntry.hashCode();
    }

    @Override
    public String toString() {
        return journalEntry.toString();
    }

    @Override
    public int compareTo(CacheJournalEntry o) {
        if(o == null || o.journalEntry == null) {
            return 0;
        }

        return Long.compare(
                journalEntry.getAppendedRecord().getMessageId(),
                o.journalEntry.getAppendedRecord().getMessageId());
    }
}
