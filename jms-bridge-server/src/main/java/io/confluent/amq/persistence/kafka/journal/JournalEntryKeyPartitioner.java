/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal;

import com.google.common.primitives.Longs;
import io.confluent.amq.persistence.domain.proto.JournalEntry;
import io.confluent.amq.persistence.domain.proto.JournalEntryKey;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Partitions JournalEntryKeys either by the Tx ID or by the record ID, depending if the record is
 * part of a transaction or not.
 */
public class JournalEntryKeyPartitioner
    implements Partitioner, StreamPartitioner<JournalEntryKey, JournalEntry> {

  private final DefaultPartitioner defaultPartitioner;

  public JournalEntryKeyPartitioner() {
    this.defaultPartitioner = new DefaultPartitioner();
  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {

    if (key instanceof JournalEntryKey) {
      //If it's a TX then partition based on the TX ID otherwise partition on the record ID
      JournalEntryKey jkey = (JournalEntryKey) key;
      return defaultPartitioner.partition(
          topic, key, keyBytes(jkey), value, valueBytes, cluster);

    }

    return defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
  }

  @Override
  public Integer partition(String topic, JournalEntryKey key, JournalEntry value,
      int numPartitions) {

    byte[] keyBytes = keyBytes(key);
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
  }

  private byte[] keyBytes(JournalEntryKey key) {
    if (key.getTxId() != 0) {

      return Longs.toByteArray(key.getTxId());
    } else {
      return Longs.toByteArray(key.getMessageId());
    }

  }

  @Override
  public void close() {
    //nothing
  }

  @Override
  public void configure(Map<String, ?> configs) {
    //nothing
  }

}
