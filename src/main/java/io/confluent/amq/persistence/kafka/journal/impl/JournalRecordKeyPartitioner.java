/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import com.google.common.primitives.Longs;
import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.JournalRecordKey;
import java.util.Map;
import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.StreamPartitioner;

/**
 * Partitions JournalRecordKeys either by the Tx ID or by the record ID, depending if the record is
 * part of a transaction or not.
 */
public class JournalRecordKeyPartitioner
    implements Partitioner, StreamPartitioner<JournalRecordKey, JournalRecord> {

  private final DefaultPartitioner defaultPartitioner;

  public JournalRecordKeyPartitioner() {
    this.defaultPartitioner = new DefaultPartitioner();
  }

  @Override
  public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes,
      Cluster cluster) {

    if (key instanceof JournalRecordKey) {
      //If it's a TX then partition based on the TX ID otherwise partition on the record ID
      JournalRecordKey jkey = (JournalRecordKey) key;
      return defaultPartitioner.partition(
          topic, key, keyBytes(jkey), value, valueBytes, cluster);

    }

    return defaultPartitioner.partition(topic, key, keyBytes, value, valueBytes, cluster);
  }

  @Override
  public Integer partition(String topic, JournalRecordKey key, JournalRecord value,
      int numPartitions) {

    byte[] keyBytes = keyBytes(key);
    return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions;
  }

  private byte[] keyBytes(JournalRecordKey key) {
    if (key.getTxId() != 0) {

      return Longs.toByteArray(key.getTxId());
    } else {

      return Longs.toByteArray(key.getId());
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
