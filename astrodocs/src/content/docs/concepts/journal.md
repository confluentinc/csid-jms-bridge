---
title: Journal
description: Learn about how journal is implemented in the JMS Bridge
tableOfContents:
  maxHeadingLevel: 5
---

## Summary
JMS Bridge implements journaling using Kafka Streams based Journal. The journaling system in Artemis is used to persist both bindings and messages.

The journal uses a write-ahead log that is used to store all the operations that are performed on the server.
This allows the server to recover from a crash and replay the operations that were lost. Note that the journal is only read back during activation / load phase to recover the state of the server and is not used as message store for message delivery during normal operation.

## Journal Architecture
Vanilla message journaling code is used up until actual Journal implementation layer. 
Kafka based Journal is used as underlying persistence mechanism but message hand over for journaling and state handling after loading from journal is still vanilla Artemis code.

Interactions with Journal on Artemis side - storing durable messages, bindings, handling load from Journals - are - from `org.apache.activemq.artemis.core.postoffice.impl.PostOfficeImpl` class (implements `PostOffice` interface), and `org.apache.activemq.artemis.core.persistence.impl.journal.AbstractJournalStorageManager` class (implements StorageManage interface).

Kafka based Journal is implemented as a Kafka Streams application with a Global State Store as a Journal and separate TX State Store for handling of JTA / XA transactions.

On write - messages are produced to Journal WAL (Write Ahead Log) topic and Ack is returned back to Active MQ and back to Producer / Consumer - as message is written to WAL even if it is not yet processed by Kafka Streams logic - it is persisted and will not be lost in case of application crash.

```plaintext
    kafka journal 
        producer 
            |
            V
        journal WAL topic    
    kafka streams
        from journal WAL topic
            |
            V
        process transactions
            |
            V
        process deletes/annotations
            |
            |--> process adds
            |       |  *** the message integration magic ***
            |       |
            |       V
            |    route messages*
            |       |
            |       V
            |    convert messages*
            |       |
            |       V
            |    publish to Kafka topics*
            V
        publish updates to journal Table topic
```

On read (application startup or activation of standby on failover) first epoch marker is produced to Journal's WAL Topic and that is used to make sure that all unprocessed events in WAL are processed into the final Journal Table topic before application loads messages into Artemis - similar to how synchronisation barriers are used.
```plaintext
    kafka journal 
        producer - epoch marker 
            |
            V
        journal WAL topic    
    kafka streams
        journal Table topic
            |
            V
        global state store
            |
            V
        wait for epoch marker
            |
            V
        load messages
        load annotations
        load transactions    
            |
            V
        load complete callback
            |
            V
    Artemis AbstractJournalStorageManager
        load messages into PageMemory / queues  
```



