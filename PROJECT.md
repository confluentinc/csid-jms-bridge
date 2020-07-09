# CSID JMS Bridge

A bridge that supports the JMS 2.0 specification and provides drop in client jars to current JMS clients to be used against it.
Has tight integration with Kafka where topic data in kafka is available via the JMS Bridge and all JMS topic data is available via Kafka.

## Deployment

Will be deployed as a standard Confluent Platform service similar to KSQLDB server. 
Requires a robust connection to Kafka.

## Relation to JMS

### Messaging Models

#### Point-to-Point (PTP)

Kafka does not support this model of messaging but the JMS-Bridge does.

You can think of this model as the same as sending an email to someone in particular (async) instead of giving them a call (sync).
It has all of the same problems as synchronous PTP communications but includes another layer that hides the recipient from the sender.
This layer can either fail, succeed or worse of all falsely succeed.
To "fix" this situation additional logic is applied to allow the sender to be notified of reciept or even wait for a reply (thus making it synchronous again).

This style of communication is error prone since the sender and recipient must always be in agreement on what their sending, where they sending and how they should reply.
These rules are either facilitated by the specification (JMS request/reply) or agreed to by the parties using an out of band communication.
This creates a tight coupling between the parties and forces them to innovate as a unit rather than independently.

This style also is not good for broadcasting information.
In order to broadcast the sender would need to know the address of all of the target recipients and have agreements on how the communication should be performed.
Although this may seem ideal for security reasons it is only "apparently" secure using the weakest form of security, that of obscurity (being hidden).
There is no true security unless true security is applied through means such as PKI.

#### Publish-and-Subscribe (PubSub)

Kafka supports this model.

This model is akin to a library, where information (books) flow through the library becoming available to anyone who wants to read them.
In the case of digital books no materials are even lost and as many people as are allowed can consume the information that they want.
They may even choose to visit the library on a regular basis to pickup any new books that become available on a subject they enjoy.

This style of communication allows for mass consumption of information.
The repository which recieves the information is responsible for categorizing and indicating what form the information is in.
It is then up to the consumers to choose from the available subjects that are in formats that they understand.

The publisher nor the subscriber know anything about each other.
The publisher may know what format is most popular with the consumers and choose to use that format but it may also support multiple formats to accommodate multiple kinds of consumers.

Since all of the information is available via the broker and is intended for any number of consumers PTP style cannot be easily supported.
It would require an individual consumer to read all of the information to find the one piece intended for them, wasting a lot of their time and resources.

### Security

JMS does not provide any specifics on security it is instead left as an implementation detail.

A particular vendor of the JMS spec would need their implementation security model compared specifically with what Kafka or the JMS-Bridge offers.

### Features

#### JMSReplyTo Header, JMSCorrelationID Header, Temporary Queues and Topics

These are JMS specified headers that are used to facilitate PTP communications.

Kafka has no built in notion of these but the JMS-Bridge does.
 
**TODO**: Decide whether PTP data should be present in kafka and how it should be made available to consumers

#### Message Headers, Properties and Body

Both Kafka and JMS have the concept of a message. Both support headers and a body but only JMS has the concept of properties.
JMS Properties are closer to what Kafka headers are, they can be any key value pair and are meant for use by the application system, not the specification.
JMS headers on the other hand are standardized and have specific meanings, this is closer to the metadata associated to a Kafka message.

Obviously one can encode JMS headers into the headers of a kafka message without issue but recieving Kafka messages into JMS would require the creation of these headers.

**TODO**: Deterministically create JMS headers based on kafka headers and metadata.
**TODO**: Define a convention for setting Kafka headers destined to be mapped to JMS message properties

#### JMSType Header

Used by JMS to allow clients to be informed of what kind of message they are receiving without inspecting the body.

This closely resembles how schema registry is integrated into Kafka.

#### Message Expiration

Each message in JMS may have an expiration set on it meaning that messages beyond their expiry time can safely be discarded by the implementation.
The JMS spec does not gaurantee that they will not be deliverd though, merely that they do not need to be delivered.

Kafka's topic retention policies are similar to this but at the topic level, not the message level.

The JMS-Bridge will honor expiry times and once a message expires will be released by it whether delivered or not.
Kafka though will retain the message for however long the corresponding retention configuration for the message's topic is.

Messages set to expire beyond Kafka's retention time will be kept available via the JMS-Bridge even after being removed from Kafka.
It would be best practice though to mirror the topic retention time to the expiry time.

**TODO**: Allow truncated (by kafka) messages to remain in the JMS-Bridge until their JMS expiration is met.

#### Message Priority

JMS has a notion of message priority which allows certain messages to be expedited over others.
It is not a required feature but one that an implementation my offer.

Kafka has no notion of this concept and can only be immulated by using priority topics instead and rely on consumers to understand the semantics.

#### Message Selectors

JMS allows a consumer to subscribe to a JMS topic then filter the incoming messages by their headers or properties.

Kafka does not support filtering of data at the broker, it instead relies on consumers to perform that work.

#### Message Body Interfaces

JMS defines several different types of message body interfaces, each corresponding to a rough description of how the body is composed.
Kafka brokers and clients know nothing of what a message contains therefore such a feature is made available via other means, e.g. the schema registry.

Since Kafka is intended to be used in polyglot systems the idea of using serialized java objects, as used in the ObjectMessage interface, makes no sense and will only be supported through custom client applications.

**TODO**: Map JMS message body interfaces to standard Avro schemas, excluding Bytes and Text.

#### Transactions

JMS supports transactions around a session which can include both consuming and publishing data.
If a transaction does not commit then the state of the data interacted with will revert back (not consumerd or published).

Kafka has transactions although they appear a bit different since they are specific to the consumer and producer.
The producer controls transactional boundaries and a consumer adheres to them.
When combining both sides one can take advantage of producer idempotence with the transaction to achieve something very similar.
Kafka streams and KSQLDB has this built into their exactly once semantics.

## Release 1.0 Milestones

### Basic Deployment Without Integration

This milestone will be met by a packaged deliverable that can be manually installed into an environment.
It will provide documentation on how to install it and configure clients to use it.

Standard ActiveMQ clients will be used with it.

Standard ActiveMQ features will be present, no attempts to hide or obfuscate the embedded ActiveMQ engine will be made it will simply be wrapped up within the JMS-Bridge application.

This milestone is only intended for discovery usage.

#### Purpose

The intentions of this milestone is to provide a very simple and quick way to validate to users that this approach can be used.
It allows them to test out updating existing clients and installing the JMS-Bridge into their environment.

Users should validate that clients can use the embedded ActiveMQ to fulfill their requirements.

Most importantly we should capture any feedback from customers about short comings or issues they encountered while attempting to integrate with it.

### JMS-Bridge Data Stored Within Kafka

For this milestone the goal is to store all JMS-Bridge logs within kafka.
This will turn the JMS-Bridge into an essentially stateless service with the exception of large message support.

Expected Changes:

 1. Configuration, it will require connection details to kafka and a principal with permissions to manage topics with a specific prefix
 2. Deployment, as long as large messages are not used disk space for data is no longer required
 3. Master/Slave support, support for master/slave HA deployment should be supported at this point


Features not part of this release include

 * large message support, since those are not stored in logs but are housed as seperate files on the JMS-Bridge's file system
 * general message synchronization, messages intended for general consumption will not be synced between Kafka topics and JMS-Bridge topics
 

#### Purpose

This release will verify that we can indeed store the JMS-Bridge logs into kafka without impacting it's feature set.
At this point we should be able to freely shutdown and spin up JMS-Bridge instances with only one ever being master.

Testing of this will be around validating: 

 1. JMS clients work as usual, with focus on:
    1. Data retention within JMS durable queues over long periods of time
    2. Expiry works as expected
 2. JMS-Bridge instances can fail and resume without issue.
 3. Kafka disk usage is reclaimed over time for the JMS-Bridge message logs

### Synching of JMS-Bridge Topics to Kafka Topics

This release will enable the ability to integrate messages originating from JMS-Bridge topics to be available to Kafka consumers.

Expected Changes:

 1. Configuration, now includes JMS-Bridge topics to Kafka topics sync setup
 2. Deployment, the JMS-Bridge Kafka principal will require write priveleges to all synced topics

Features:

 1. JMS-Bridge topic data available via Kafka (text and binary only)
 1. transaction support, meaning data will not be available to Kafka consumers until the JMS transaction is committed
 1. acyclic gaurantee, i.e. prevention of infinite loops of data between Kafka and the JMS-Bridge

Features not included:

 1. JMS message type support for kafka clients, only text and binary message types will be supported
 1. Support of message features such as expiry within kafka, normal retention policies will be used

#### Purpose

With this release full integration of data between Kafka and the JMS-Bridge will be available.
It will be limited to text and binary message types only.

Users should validate:

 1. JMS client data is available via kafka topics as configured
 2. JMS transactional data is not available until a commit has occurred
 3. Validate that messaging guarantees are being met (at-least-once)


### Synching of Kafka Topics to JMS-Bridge Topics

Upon this release a JMS client should be able to receive all data found in Kafka topics in similarily defined JMS topics.
A configuration will be made available to setup the exposure of these topics to the JMS-Bridge with each configuration including information on the type of JMS message it should be converted to.

Expected Changes:

 1. Configuration, now includes topic syncing setup
 2. Deployment, permissions must be granted to the JMS-Bridge Kafka principal to consume from all synced topics

Features:

 1. Integration of Kafka topics to JMS-Bridge topics as configured
 2. Conversion of Kafka topic data to JMS message types as configured

Features not included:
 * large message support
 * synchronization of message originating from JMS-Bridge to kafka topics

#### Purpose

To allow Kafka originating data to be used by JMS clients without code modifications.

Users should validate that: 

 1. message conversion works as expected and clients do not have issues with consuming them
 2. incorrect topic sync configuration is promptly and effectively communicated
 3. that sudden JMS-Bridge failure does cause Kafka originating messages to be missed (although duplication may occur)


### Clustering

This milestone will include validation and verification of clustering support for the JMS-Bridge.


Features include:

  1. Information on how to setup a supported cluster
  2. How to roll a cluster for upgrades
  3. Cluster failover and recovery
  4. Cluster telemetry


## Release 1.x Milestones

The above will represent the first major release candidate of the JMS-Bridge

### JMS Message Types Support

This release will include support for JMS message types across both the JMS-Bridge and Kafka clients.
It will use the schema-registry and avro to define the message types so Kafka clients are able to identify and correctly read them.

Features include:
 1. Full support of all JMS message types across both the JMS-Bridge and Kafka clients

Expected Changes:

 1. Schema registry will be required
 1. Kafka clients will need to support avro and the RecordNameStrategy
 1. Schemas for JMS message types will need to be registered using the RecordNameStratgy
 1. Configuration
    1. topic setup in JMS-Bridge can have any conversion configuration removed
    1. schema registry will need to be added

### Message Disk Use Reduction

This release will eliminate the duplication of message data within kafka by removing the messages from the JMS-Bridge journals.
This will be done by using references to messages in kafka topics instead of storing the entire message.

#### Purpose

To reduce disk usage on kafka.

Users should validate:

    1. Message expiry and JMS Queue retention still function as expected
    1. JMS-Bridge startup times are not impacted severly
    1. Kafka disk usage has for JMS-Bridge topics has reduced significantly

### Large Message Support

In this milestone the ability to store arbitrarily sized messages (via JMS) will be restored.

### Unified Security

This milestone will synchronize security between kafka and the JMS-Bridge for end user clients.

