---
section: Guides
title: Client Integration
description: Learn how to integrate the JMS Bridge with your applications
---

## Jar Installation

The client jar can be found in the release archive, it is compatible with Java 1.8 and newer.
In the `lib/jms-bridge/client/` directory you'll find a jar file named `jms-bridge-client-all-<version>.jar`, this contains the client.
To use the client you must add it to your applications classpath.

> Summary
>
> - add `<release>/lib/jms-bridge/client/jms-bridge-client-all-<version>.jar` to your applications classpath

## Replacing an Existing JMS Implementation

To replace an existing JMS implementation within an application you must:

1.  Remove the current implementation jar from the applications classpath
1.  Add the above client-jar to the classpath
1.  Restart the application

This works as long as the application did not depend on any vendor specific extensions to the JMS specification.

## Using Maven Dependency

You can also get the client jar via a maven dependency.
Since the JMS-Bridge uses an embedded version of the popular ActiveMQ Artemis Broker you can use it as an equivelant replacement of the jms-bridge client jar.
In fact, they are the same thing.
Just make sure to get the right version, it should match the version found in the name of the jms-bridge client jar.

_maven_

```xml
<dependency>
    <groupId>org.apache.activemq</groupId>
    <artifactId>artemis-jms-client-all</artifactId>
    <version>2.13.0</version>
</dependency>
```

_gradle_

```groovy
compile group: 'org.apache.activemq', name: 'artemis-jms-client-all', version: '2.13.0'
```

## Client Usage

See the artemis documentation for many examples of using the JMS client jar.

https://activemq.apache.org/components/artemis/documentation/latest/using-jms.html
