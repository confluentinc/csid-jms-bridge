/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.persistence.kafka.journal.impl;

import io.confluent.amq.persistence.kafka.JournalRecord;
import io.confluent.amq.persistence.kafka.KafkaRecordUtils;
import io.confluent.amq.persistence.kafka.ReconciledMessage;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.LongAdder;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaJournalStoreLoader implements
    KafkaJournalHandler, StateRestoreListener, KeyValueStore<Bytes, byte[]> {

  public static Supplier createSupplier(String name, KafkaJournalTxHandler txHandler) {
    return new Supplier(name, txHandler);
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaJournalStoreLoader.class);
  private static final KeyValueIterator<Bytes, byte[]> EMPTY_ITER =
      new KeyValueIterator<Bytes, byte[]>() {

        @Override
        public void close() {
        }

        @Override
        public Bytes peekNextKey() {
          return null;
        }

        @Override
        public boolean hasNext() {
          return false;
        }

        @Override
        public KeyValue<Bytes, byte[]> next() {
          throw new NoSuchElementException();
        }
      };

  private final String name;
  private final KafkaJournalTxHandler txHandler;
  private final KafkaJournalReconciler reconciler;
  private final CompletableFuture<KafkaJournalLoaderCallback> loaderReady;

  private final LongAdder count;
  private final AtomicInteger finalLoadCount = new AtomicInteger(0);
  private final AtomicInteger restoreCounter = new AtomicInteger(0);
  private volatile boolean open = false;

  public KafkaJournalStoreLoader(String name, KafkaJournalTxHandler txHandler) {
    this.name = name;
    this.txHandler = txHandler;
    this.reconciler = new KafkaJournalReconciler(this, txHandler, this.name);
    this.count = new LongAdder();
    this.loaderReady = new CompletableFuture<>();
  }

  public void readyLoader(KafkaJournalLoaderCallback callback) {
    loaderReady.complete(callback);
  }

  ////// KafkaJournalHandler impl
  @Override
  public List<ReconciledMessage<?>> handleRecord(byte[] key, JournalRecord record) {
    //Forwards add/update records
    return Collections.singletonList(ReconciledMessage.forward(this.name, key, record));
  }


  /////// StateRestoreListener impl

  @Override
  public void onRestoreStart(TopicPartition topicPartition, String storeName, long startingOffset,
      long endingOffset) {
    if (this.name.equals(storeName)) {
      restoreCounter.getAndIncrement();
    }
  }

  @Override
  public void onBatchRestored(TopicPartition topicPartition, String storeName, long batchEndOffset,
      long numRestored) {
    //do nothing
  }

  @Override
  public void onRestoreEnd(TopicPartition topicPartition, String storeName, long totalRestored) {
    if (this.name.equals(storeName)) {
      int currCount = restoreCounter.decrementAndGet();
      if (currCount < 1) {
        int finalCount = restoreCounter.getAndSet(Integer.MAX_VALUE);
        finalLoadCount.compareAndSet(0, finalCount);
      }
    }
  }


  /////// KeyValueStore impl

  @Override
  public void put(Bytes o1, byte[] o2) {
    count.increment();
  }

  @Override
  public byte[] putIfAbsent(Bytes o, byte[] o2) {
    //no-op
    return null;
  }

  @Override
  public void putAll(List<KeyValue<Bytes, byte[]>> list) {
    count.add(list.size());
  }

  @Override
  public byte[] delete(Bytes o) {
    count.decrement();
    return null;
  }

  @Override
  public byte[] get(Bytes o) {
    //no-op
    return null;
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> range(Bytes o, Bytes k1) {
    return EMPTY_ITER;
  }

  @Override
  public KeyValueIterator<Bytes, byte[]> all() {
    return EMPTY_ITER;
  }

  @Override
  public long approximateNumEntries() {
    return count.sum();
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public void init(ProcessorContext processorContext, StateStore stateStore) {
    open = true;
    processorContext.register(stateStore, (k, v) -> {
      count.increment();
      restore(k, convertRestoreInput(v, false));
    });
  }

  protected ByteBuffer convertRestoreInput(byte[] data, boolean isKey) {
    if (data == null) {
      return null;
    }

    if (isKey) {
      return ByteBuffer.wrap(data);
    }

    if (data.length >= 8) {
      return ByteBuffer.wrap(data, 8, data.length - 8);
    } else {
      return ByteBuffer.wrap(data);
    }
  }

  protected void restore(byte[] key, ByteBuffer value) {
    try {
      JournalRecord journalRecord = JournalRecord.parseFrom(value);

      //use our reconciler which also forwards add/update records
      List<ReconciledMessage<?>> messages = this.reconciler.reconcileRecord(key, journalRecord);

      //now we need to make sure the loader callback is available
      KafkaJournalLoaderCallback callback = loaderReady.get();

      for (ReconciledMessage<?> rawMsg: messages) {
        ReconciledMessage<JournalRecord> rmsg = rawMsg.asForward();
        if (rmsg != null) {
          switch (rmsg.getValue().getRecordType()) {
            case DELETE_RECORD:
              callback.deleteRecord(rmsg.getValue().getId());
              break;
            case ADD_RECORD:
              callback.addRecord(KafkaRecordUtils.toRecordInfo(rmsg.getValue()));
              break;
            case UPDATE_RECORD:
              callback.updateRecord(KafkaRecordUtils.toRecordInfo(rmsg.getValue()));
              break;
            default:
              LOGGER.warn("Unwanted record type received during restore/load: {}",
                  rmsg.getValue().getRecordType());
              break;
          }
        }
      }

      //check to see if restore is over
      if (restoreCounter.get() == Integer.MAX_VALUE) {
        this.txHandler.preparedTransactions().forEach(callback::addPreparedTransaction);
        callback.loadComplete(finalLoadCount.get());
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void flush() {
    //no-op
  }

  @Override
  public void close() {
    open = false;
  }

  @Override
  public boolean persistent() {
    return false;
  }

  @Override
  public boolean isOpen() {
    return open;
  }


  public static class Supplier implements KeyValueBytesStoreSupplier {

    private final String name;
    private final KafkaJournalTxHandler txHandler;

    public Supplier(String name, KafkaJournalTxHandler txHandler) {
      this.name = name;
      this.txHandler = txHandler;
    }

    @Override
    public String name() {
      return name;
    }

    @Override
    public KeyValueStore<Bytes, byte[]> get() {
      return new KafkaJournalStoreLoader(name, txHandler);
    }


    @Override
    public String metricsScope() {
      return "amq";
    }
  }
}
