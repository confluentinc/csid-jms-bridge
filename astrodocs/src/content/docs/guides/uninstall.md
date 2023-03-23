---
section: Guides
title: Uninstalling The JMS Bridge
description: Learn how to uninstall the JMS Bridge
---

## Linux Terminal

To remove the JMS-Bridge itself from Linux execute on terminal:

1. Stop installed broker running via

   ```shell
   kill -9 <pid>
   ```

2. `Optional:` Remove / Delete the JMS Broker Directory

   ```shell
   rm -rf /jms-bridge-server-0.1.0-SNAPSHOT/*
   ```
