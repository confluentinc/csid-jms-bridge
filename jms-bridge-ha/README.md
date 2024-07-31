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
    - kafka exposed on: `localhost:29092`, `kafka:9092`
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

### Step 2. Start a kafka consumer

In a new terminal, start a kafka consumer (this example uses kcat) to consume
messages from the kafka topic.

```bash
kcat -C -b localhost:29092 -t test
```

You can use this to verify that kafka has persisted all messages passing through the bridge.

### Step 3. Tail the producer logs

In a new terminal, start the producer. This producer will send messages to jms bridge.

```bash
docker compose logs -f producer
```

### Step 4. Tail the consumer logs

In a new terminal, start the consumer. This consumer will consume messages from jms bridge.

```bash
docker compose logs -f consumer
```

### Step 4. Shut down the live server

In a new terminal, shut down the live server.

```bash
docker compose stop live-jms-bridge
```

If you look at the backup-jms-bridge logs, you'll see the
log ` [AMQ229000: Activation for server ActiveMQServerImpl::name=localhost] INFO  o.a.activemq.artemis.core.server -- AMQ221010: Backup Server is now live`
indicating that the backup server has taken over.

### Step 5. Start the live server

After you've observed clients failing over (typically indicated by the producer printing a message detecting a
disconnect _"Trying again..."_), start the live server back up.

```bash
docker compose start live-jms-bridge
```

if you look at live-jms-bridge logs you'll see the log `AMQ221035: Live Server Obtained live lock` indicating that the
live server has taken back over. and that the backup server logs has `AMQ221031: backup announced`.

## Testing HA with ToxiProxy for Kafka connectivity loss from one node.

### Step 1 - setup node to go through proxy.
update `docker-compose.yml` for the live or backup jms node to use toxiproxy:9093 instead of kafka:9092 for the node that you want to be able to control Kafka connectivity for.
```bash
JMSBRIDGE_KAFKA_BOOTSTRAP_SERVERS: kafka:9092
```
to
```bash
JMSBRIDGE_KAFKA_BOOTSTRAP_SERVERS: toxiproxy:9093
```

### Step 2 - start Kafka and ToxiProxy docker containers

```bash
docker compose up -d kafka
docker compose up -d toxiproxy
```

### Step 3 - create a proxy rule for routing traffic to Kafka

```bash
docker run --rm --network=jms-bridge-ha_default --entrypoint="/toxiproxy-cli" -it ghcr.io/shopify/toxiproxy -h toxiproxy:8474 create -l 0.0.0.0:9093 -u kafka:9093 kafka
```

### Step 4 - when ready to kill connectivity from node configured for use of proxy to Kafka - run the following command

```bash
docker run --rm --network=jms-bridge-ha_default --entrypoint="/toxiproxy-cli" -it ghcr.io/shopify/toxiproxy -h toxiproxy:8474 delete kafka 
```

### Step 5 - Verify the logs / that failback / failover happened as expected
Check logs for the live and backup jms bridge nodes, consumer and producer for clients to see the failover / failback process.
```bash
docker compose logs -f live-jms-bridge
```
```bash
docker compose logs -f backup-jms-bridge
```
```bash
docker compose logs -f producer
```
```bash
docker compose logs -f consumer
```

### Step 6 - Optionally - recreate the proxy rule to restore connectivity / test failback. 
```bash
docker run --rm --network=jms-bridge-ha_default --entrypoint="/toxiproxy-cli" -it ghcr.io/shopify/toxiproxy -h toxiproxy:8474 create -l 0.0.0.0:9093 -u kafka:9093 kafka
```
Restart the crashed node as needed.
```bash
docker compose up -d
```
