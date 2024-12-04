---
title: High Availability
description: Learn about how high availability is implemented in the JMS Bridge
tableOfContents:
  maxHeadingLevel: 5
---

## Summary

High availability is a feature that allows the JMS Bridge to continue to operate even if one of the nodes in the cluster fails. This is achieved by having a standby node that can take over the work of the failed node. 

Implementation is based on Artemis Shared Store Master-Slave high availability mode with important difference of shared filesystem store replaced by Kafka Streams based shared journal and Kafka Consumer Membership protocol replacing file based locks.

## High Availability Architecture
JMS Bridge re-uses Shared Store activations of Artemis - but instead of using shared filesystem - Kafka Streams stateful Journal that is solely dependent on Kafka is used for Journaling operations and File based locks are replaced by Kafka Consumer Group Membership protocol - the actual flow of operations stays exactly the same.

Typical startup and failover process in Primary / Backup HA setup:
```mermaid
sequenceDiagram
    participant JMS Producer
    participant JMS Consumer
    participant Artemis Live
    participant Journal
    participant HALock
    participant Artemis Backup
    HALock->>HALock:Initial state 'NOT_STARTED'
    Artemis Live->>+HALock:Acquire Live Lock
    HALock->>-Artemis Live:Live Lock Acquired
    Artemis Backup->>+HALock:Acquire Backup Lock
    HALock->>-Artemis Backup:Backup Lock Acquired
    Artemis Backup->>+HALock:Await State != 'NOT_STARTED'
    Artemis Live->>HALock:Update State to 'Live'
    HALock->>-Artemis Backup:Await for State released
    Artemis Backup->>+HALock:Acquire Live Lock
    Artemis Live->>+Journal:Read Journal
    Journal->>-Artemis Live:Done
    Artemis Live->>Artemis Live:Startup done, Accept connections
    JMS Consumer->>Artemis Live: Subscribe
    JMS Producer->>+Artemis Live:Produce Msg1
    Artemis Live->>Journal: Store Msg1
    Artemis Live->>-JMS Producer:Ack Msg1
    Artemis Live->>JMS Consumer:Send Msg1
    JMS Consumer->>Artemis Live: Ack Msg1
    Artemis Live->>Journal: Delete Msg1
    JMS Producer->>+Artemis Live: Produce Msg2
    Artemis Live->>Journal: Store Msg2
    Artemis Live->>-JMS Producer: Ack Msg2
    Artemis Live->>Artemis Live: Crash
    HALock->>-Artemis Backup:Live Lock Acquired
    Artemis Backup->>Journal: Load Journal
    Journal->>Artemis Backup: Done
    Artemis Backup->>Artemis Backup: Startup Done, Accept Connections
    JMS Consumer->>Artemis Backup: Re-connect (Failover)
    JMS Producer->>Artemis Backup: Re-connect (Failover)
    Artemis Backup->>JMS Consumer:Send Msg2
    JMS Consumer->>Artemis Backup: Ack
    Artemis Backup->>Journal:Delete Msg2
```

