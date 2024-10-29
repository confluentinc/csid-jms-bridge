---
title: Architecture
description: Learn about the architecture of the JMS Bridge
tableOfContents:
  maxHeadingLevel: 5
---

This is an overview of how different features within the JMS-Bridge were implemented.

## Message Integration to Kafka

A major component of the JMS Bridge is to allow interoperability between JMS clients and Kafka clients.
To do this involves publishing JMS messages to Kafka so they are available to the clients there.
Several aspects need to be considered when doing this.

1. Conversion of JMS message types to Kafka messages
2. Preservation of JMS message metadata
3. Whether to propagate non-durable JMS messages
4. Mapping of JMS topics to Kafka topics
5. Extraction of Kafka message key from JMS messages

Also we must consider that JMS supports point-to-point queue semantics where as Kafka only supports the pub-sub model.
As a follow-up we may want to integrate PTP semantics via a convention that can be used by kafka clients.

1. Support for PTP semantics (request/reply)

### Current JMS Bridge Integration

To integrate in this feature we will use the already existing kafka streams topology found in the JMS-Bridge.
Instead of ignoring newly added messages those messages will be routed and published to kafka by extending the existing
topology.

```plaintext
    kafka streams
        from journal topic
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
            |    publish to topics*
            V
        publish updates to journal topic
```

### Routing Messages

Initial implementation of routing will be simplistic and hands on.
It will require the JMS-Bridge to be configured with information about:

- A route that contains
    1. a message filter predicate that when true marks it as a part of this routing
        - JMS address, JMS header value
    2. a destination kafka topic that the message should be published to
    3. a conversion mechanism informing how it should be converted
        - Kafka key to extract from the message

#### Assumptions

1. The destination kafka topic has been created and exists
2. The JMS-Bridge kafka principal has permission to publish to the topic
3. The JMS messages are either binary or text

#### Limitations

A message may only be routed to a single kafka topic and it will be derived from the first route that matches.
The order of the routing will be maintained from how it is ordered in the configuration.

Due to asynchronous nature of Kafka Streams runtime - messages can be re-produced to Kafka Topics after JMS Bridge node crash - causing duplication on Kafka Topics only - see [JMS Kafka divert](/concepts/jms_kafka_divert/) for more information.

### Converting Messages

All JMS headers will be converted to corresponding Kafka message headers using a standard convention.
The convention will be:

```plaintext
jms.<jms_property_name>

//e.g.
MessageId -> jms.MessageId
```

A JMS Bridge origin header will also be added to aid in the prevention of data cycles.

```properties
jmsbridge.origin=<bridge.id>
```

As part of the conversion a key for the message will be selected for publishing to kafka.
The choice of this key is important since it determines how Kafka's ordering guarantees are enforced.
Initially it can be configured to use any available JMS header with a text or numeric value as the key.
If no key is specified then the `MessageId` will be used.

Since only text and binary messages will be propagated at this time the contents of those messages will be published
as-is to kafka.

### Configuring Routes

Routes will be configured in the `jms-bridge.properties` file.
Each route will require a unique name, routing predicate, destination kafka topic and conversion options.
The configuration will be processed from the top down, any duplicate keys will override the preceding one.

```properties
routing.dead-letter-topic=jms-bridge-to-kafka-dead-letter-topic
routes.fooRoute.name=fooRoute
routes.fooRoute.in.include=msg.address==foo-topic
routes.fooRoute.out.topic=foobar-topic
routes.fooRoute.conv.key=msg.header.CorrelationId
routes.barRoute.name=barRoute
routes.barRoute.in.include=msg.address==bar-topic
routes.barRoute.out.topic=foobar-topic
routes.barRoute.map.key=msg.header.CorrelationId
```

### Error Handling

There are two kinds of failures, hard failures and soft failures.
Hard failures are ones that cannot be recovered from and include:

1. Invalid kafka topic destination
2. Not authorized to write to the kafka topic

These will cause a hard stop of the jms-bridge.

Soft failures on the other hand are usually data related.
These include:

1. Key mapping invalid (no key found, key value isn't alphanumeric)
2. Conversion failure (message type is not text/binary)

These can be configured to either cause a hard stop (default) or continue processing.
Options that allow for continued processing after encountering the error include:

1. Log and Skip the message
2. Log and dead letter the message

Dead lettered messages will be published to another kafka topic which can be later inspected.
The `routing.dead-letter.topic` can be used to configure which topic to publish to in Kafka.
If for some reason publishing to that topic fails then a hard stop will occur.

### Telemetry

Telemetry for the routing can be found within the Kafka Streams JMX beans. Custom metrics include:

- **Routing Latency Average**
    - Metric: `stream-jms-bridge-metrics[jms-bridge-id=router][routing-latency-avg]`
    - Description: The average time it takes to route and convert the record.

- **Routing Success Rate**
    - Metric: `stream-jms-bridge-metrics[jms-bridge-id=router][routing-success-rate]`
    - Description: The number of successful routings since the server started.

- **Routing Failure Rate**
    - Metric: `stream-jms-bridge-metrics[jms-bridge-id=router][routing-failure-rate]`
    - Description: The number of failed routings since the server started.
