---
title: Configuration
description: Learn about the configuration of the JMS Bridge
slug: 3.3.5/reference/configuration
---

All configuration for the JMS Bridge is done via a file which is supplied to the [jms-bridge-server-start](https://github.com/confluentinc/csid-jms-bridge/tree/master/bin/jms-bridge-server-start) script.
An example configuration can be found in [etc/jms-bridge.conf](https://github.com/confluentinc/csid-jms-bridge/tree/master/config/jms-bridge.conf).

The configuration is written using Lightbends config library and file format, HOCON (see https://github.com/lightbend/config).
HOCON is fairly straight forward and is compatible with JSON and even java properties but it also has more advanced features which can simplify configuration.
Use the default configuration as a reference when customizing it your own.

## Artemis Configuration

Since the underlying JMS engine is a customized embedded Apache ActiveMQ Artemis broker one can configure that directly.
To do so requires editing the [broker.xml](https://github.com/confluentinc/csid-jms-bridge/tree/master/config/broker.xml) file found in the [etc](https://github.com/confluentinc/csid-jms-bridge/tree/master/config) directory.
The [broker.xml](https://github.com/confluentinc/csid-jms-bridge/tree/master/config/broker.xml). file must be located next to the configured [etc/jms-bridge.properties](https://github.com/confluentinc/csid-jms-bridge/tree/master/config/jms-bridge.properties.template) file or it will not be found.

A default [broker.xml](https://github.com/confluentinc/csid-jms-bridge/tree/master/config/broker.xml) is supplied with the installation.
Feel free to update it as desired.
The reference for that file can be found on the Artemis documentation site:
https://activemq.apache.org/components/artemis/documentation/latest/configuration-index.html

## Configuration Reference

See the default configuration file [etc/jms-bridge.conf](https://github.com/confluentinc/csid-jms-bridge/tree/master/config/jms-bridge.conf) for information on all of the configuration options.
