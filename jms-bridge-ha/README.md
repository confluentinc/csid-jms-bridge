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

This exercise will showcase how to setup a live - backup group with 1 live server and 1 backup servers using shared
storage. We will have a client producing messages and a client consuming messages (persistent) that will be validated
based on sequence (they may be consumed out of order). We will simulate a failover by shutting down the live server and
observing the backup server take over, and verify messages are still flowing through without any messages being lost.

### Step 1.