---
title: JMS Kafka Divert Limitations
description: Sequence Diagram to show JMS Kafka Divert processing on crash
tableOfContents: false
head:
  - tag: style
    content: ":root { --sl-content-width: 100% !important; }"
---

## JMS Producer to Kafka Topic Divert Processing on Crash Flow

Following flow highlights limitation that asynchronous processing of divert in Kafka Streams based message journal can have upon crash of JMS Bridge node.
Kafka Streams processing state is saved periodically (known as Commit) and upon crash, the state is restored from the last commit point. Due to this, messages can be re-produced to Kafka Topics after JMS Bridge node crash - causing duplication on Kafka Topics only.

Sequence diagram below illustrates a flow where:
- Message 1 is sent by JMS Producer
- Message 1 is ingested into Journal
- Message 1 is Acked back to JMS Producer
- Message 1 is diverted to Kafka
- KStreams state committed
- Messages 2,3 are ingested into Journal
- Messages 2,3 are Acked back to JMS Producer
- Messages 2,3 are diverted to Kafka
- Message 4 is sent by JMS Producer
- Message 4 is ingested into Journal
- JMS Bridge crashes
- Standby JMS Bridge takes over
- KStreams state is restored from last commit point 
- Messages 2,3 are re-processed and diverted to Kafka again (causing duplicates on Kafka Topic)
- Message 4 is diverted to Kafka
- JMS Producer sends Message 4* again (retry as Ack was not returned from JMS Bridge due to crash even though message was persisted to Journal)
- Message 4* is ingested into Journal again (retry - duplicate on JMS Queue / Topic)
- Message 4* is Acked back to JMS Producer (retry)
- Message 4* is diverted to Kafka (retry - duplicate on Kafka Topic)

The handling of Message 4 is consistent with Vanilla Artemis and guarantees in Sync send mode, but duplication caused by reprocessing divert for messages 2 and 3 are side effects of Kafka Streams processing and asynchronous state commit.

```mermaid
sequenceDiagram
    JMS Producer->>+JMS Bridge: Sync Send 1
    JMS Bridge->>+Message Journal: Kafka Produce 1 (Ingest)
    Message Journal-->>-JMS Bridge: Kafka Produce 1 Ack
    JMS Bridge-->>-JMS Producer: JMS Send 1 Ack
    Message Journal->>Kafka Divert Processor: Apply Divert 1
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 1
    Message Journal->>Message Journal: Commit KStream State
    Note over JMS Bridge,Kafka Divert Processor: Save point for Journal Processing - KStream Commit
    JMS Producer->>+JMS Bridge: Sync Send 2
    JMS Bridge->>+Message Journal: Kafka Produce 2 (Ingest) 
    Message Journal-->>-JMS Bridge: Kafka Produce 2 Ack
    JMS Bridge-->>-JMS Producer: JMS Send 2 Ack
    JMS Producer->>+JMS Bridge: Sync Send 3
    JMS Bridge->>+Message Journal: Kafka Produce 3 (Ingest)
    Message Journal-->>-JMS Bridge: Kafka Produce 3 Ack
    JMS Bridge-->>-JMS Producer: JMS Send 3 Ack
    Message Journal->>Kafka Divert Processor: Apply Divert 2
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 2
    Message Journal->>Kafka Divert Processor: Apply Divert 3
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 3
    JMS Producer->>+JMS Bridge: Sync Send 4
    JMS Bridge->>+Message Journal: Kafka Produce 4 (Ingest)
    Message Journal-->>-JMS Bridge: Kafka Produce 4 Ack
    Note over JMS Bridge,Kafka Divert Processor: JMS Bridge Crashes
    Note over JMS Bridge,Kafka Divert Processor: Failover to Standby JMS Bridge
    Note over JMS Bridge,Kafka Divert Processor: KStreams committed state loaded
    Message Journal->>Kafka Divert Processor: Apply Divert 2
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 2
    Message Journal->>Kafka Divert Processor: Apply Divert 3
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 3
    JMS Producer->>+JMS Bridge: Sync Send 4* <br/> (retry as ACK not received)
    JMS Bridge->>+Message Journal: Kafka Produce 4* (Ingest) <br/>(retry)
    Message Journal-->>-JMS Bridge: Kafka Produce 4* Ack <br/>(retry)
    JMS Bridge-->>-JMS Producer: JMS Send 4* Ack <br/>(retry)
    Message Journal->>Kafka Divert Processor: Apply Divert 4 <br/>(original)
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 4 <br/>(original)
    Message Journal->>Kafka Divert Processor: Apply Divert 4* <br/>(retry)
    Kafka Divert Processor->>Kafka Topic: Kafka Produce 4* <br/>(retry)
```
