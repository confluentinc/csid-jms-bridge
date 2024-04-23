# JMS Bridge High Availability Walkthrough

This exercise will walk you through how jms bridge handles high availability.

## Prerequisites

- see [jms-bridge-docker](../jms-bridge-docker/README.md) for building a local jms bridge image
- Docker w/ Docker Compose

## Overview

JMS Bridge allows servers to be linked together as live - backup groups where each live server can have 1 or more
backup servers. A backup server is owned by only one live server.

Backup servers are not operational until failover occurs, however 1 chosen backup, which will be in passive mode,
announces its status and waits to take over the live servers work.

Before failover, only the live server is serving the Apache ActiveMQ Artemis clients while the backup servers remain
passive or awaiting to become a backup server.

When a live server crashes or is brought down in the correct mode, the backup server currently in passive mode will
become live and another backup server will become passive.

If a live server restarts after a failover then it will have priority and be the next server to become live when the
current live server goes down, if the current live server is configured to allow automatic fallback then it will detect
the live server coming back up and automatically stop.

## What's included

The docker compose file will stand up the following:

1. A single node kafka and zookeeper cluster
    - kafka exponsed on: `localhost:29092`, `kafka:9092`
    - On startup, a topic `test` will be pre-created and made available for use.
2. A jms bridge cluster with 1 live server and 1 backup server.
    - live server exposed on: `localhost:61616`
    - backup server exposed on: `localhost:61617`
3. A sample jms producer and consumer client that will produce and consume messages from the jms bridge.
    - **NOTE** the topic `test` we will use is pre-created on the kafka cluster as part of docker compose startup
    - The producer and consumer clients are written in Java and use the `jbang` tool to run them (wrapper included).
    - jms topics are prefixed with `kafka.`. To avoid confusion, the clients have been modified to already include this
      prefix when specifying topic names.

### Step 1. Start the docker containers

```bash
./set-env.sh

docker compose up -d
```

### Step 2. Start the consumer

This consumer will consume messages from the jms bridge.

```bash
jbang Consumer.java -t test
```

### Step 3. Start a kafka consumer

In a new terminal, start a kafka consumer (this example uses kcat) to consume
messages from the kafka topic.

```bash
kcat -C -b localhost:29092 -t test
```

### Step 4. Start the producer

In a new terminal, start the producer. This producer will send messages to jms bridge.

```bash
jbang Producer.java -t test hello
```

### Step 5. Shut down the live server

In a new terminal, shut down the live server.

```bash
# a way to kill the broker without removing the host
docker compose pause live-jms-bridge
```
