---
section: Guides
title: Performance tuning and sizing
description: Performance tuning and sizing considerations for JMS Bridge
---

## Introduction
JMS Bridge is largely based on Artemis MQ so performance tuning and sizing guides for Artemis are still largely applicable to JMS Bridge as well - with main exclusion being filesystem tuning for shared storage - as JMS Bridge is using Kafka for journaling instead of shared storage.

It should be noted that usage profile specifics will considerably affect the broker node sizing and performance - therefore the guides are to be used as rough estimates and starting points for further tuning.

## Artemis MQ Guides 
Artemis MQ docs - [performance tuning page](https://activemq.apache.org/components/artemis/documentation/latest/perf-tuning.html) - as mentioned - storage / filesystem tuning considerations are not applicable to JMS Bridge.

Amazon MQ benchmark / sizing guide - [Amazon MQ benchmarking and sizing guide](https://docs.aws.amazon.com/amazon-mq/latest/developer-guide/benchmarking-sizing.html) - Artemis has similar characteristics and the results are in the same ballpark as the Amazon MQ so this guide is also applicable to JMS Bridge for rough sizing estimation.

## Considerations and tuning of Kafka integration
As JMS Bridge uses Kafka Streams for Journaling - provisions should be made for a KStreams State Store - which is a local RocksDB instance and should be sized according to expected message journal size.

Default Kafka configuration is usually sufficient for most use cases - but if you are experiencing performance issues - consider tuning Kafka configuration as well - as tuning will be dependent on the specific use case and expected load - the recommended approach is to collect metrics and benchmark the system under load to identify bottlenecks and then tune accordingly.

Kafka Streams operations guide - [Kafka Streams operations guide](https://docs.confluent.io/platform/current/streams/sizing.html)

Kafka Streams configuration reference - [Kafka Streams configuration reference](https://docs.confluent.io/platform/current/streams/developer-guide/config-streams.html)

General Confluent Kafka Clients tuning guide - [Confluent Kafka Clients tuning guide](https://docs.confluent.io/cloud/current/client-apps/optimizing/index.html)

Client configuration reference - https://docs.confluent.io/cloud/current/client-apps/client-configs.html 
