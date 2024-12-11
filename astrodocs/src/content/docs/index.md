---
title: Introduction
description: Learn about the JMS Bridge
---

Connect legacy JMS based applications to the Confluent Platform without major modifications.

![Overview Diagram](../../assets/overview-diagram.png)

## Overview

The JMS-Bridge is a component that can be used to facilitate quicker migration from legacy JMS based systems to ones built around the Confluent Platform. It is quite common for enterprise systems to use JMS as a means of integrating external applications to a central system. These applications are usually not maintained by the system owners but by external teams which may not share the same goals or priorities of the system team. This creates a problem when the system team wants to migrate away from their legacy JMS vendor to the Confluent Platform since it would require updating all of those external clients.

Easing this transition is the goal of the JMS-Bridge. By providing a fully compliant JMS 2.0 implementation of the client jars and a server side component it can accommodate the existing patterns of those external JMS applications. With tight integration to the Confluent Platform all JMS originated topic data will be available in Kafka and all Kafka topic data will be available in JMS topics. Since the JMS-Bridge is built on top of the Confluent Platform, using it as it's storage mechanism, it does not require additional disk space or SAN provisioning, if you are monitoring Kafka you are monitoring the JMS-Bridge.

## Features

- Full JMS 2.0 compliance
- All data stored in Kafka
- Full integration of topic data between Kafka and JMS-Bridge
    - Publish from JMS, Consumer from Kafka
    - Publish from Kafka, Consume from JMS

## Benefits

- Transition from JMS to the Confluent Platform with minor updates to existing JMS applications
    - client jar update
- Quickly start innovating using Kafka and the Confluent Platform
  \*Allow ample time to either migrate legacy JMS applications or allow them to naturally fade away

## Built on Artemis MQ

The JMS-Bridge is built on [Artemis MQ](https://activemq.apache.org/components/artemis/documentation/latest/messaging-concepts.html#messaging-concepts), a robust and high-performance messaging system that serves as the foundation for its JMS 2.0 compliant implementation. By leveraging Artemis MQ, the JMS-Bridge inherits advanced features such as efficient [message routing](https://activemq.apache.org/components/artemis/documentation/latest/address-model.html), client failover capabilities, and [flexible message persistence](https://activemq.apache.org/components/artemis/documentation/latest/persistence.html). These features ensure that the bridge can reliably handle enterprise-grade workloads while maintaining seamless integration with Confluent.

Key differences and enhancements include:


Failover Support: Moving the Artemis MQ’s built-in failover mechanisms from shared store to Kafka based implementation, the JMS-Bridge ensures high availability and resilience, allowing seamless recovery from node or connection failures without data loss without complexity of SAN / shared storage architecture.

Protocol Compatibility: While based on Artemis MQ, the JMS-Bridge introduces tight coupling with Kafka, ensuring bi-directional data flow. This enables legacy JMS applications to interact directly with Kafka topics as if they were native JMS destinations.

By combining the proven messaging capabilities of Artemis MQ with the scalability and flexibility of Kafka, the JMS-Bridge offers a robust solution for bridging legacy JMS systems to Confluent. This architecture not only accelerates migration efforts but also provides a reliable and performant integration layer.