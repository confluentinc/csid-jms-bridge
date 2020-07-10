# Configuration

All configuration for the JMS Bridge is done via a properties file which is supplied to the `jms-bridge-server-start` script.
An example configuration can be found in `etc/jms-bridge/jms-bridge.properties`.

## Artemis Configuration

Since the underlying JMS engine is a customized embedded Apache ActiveMQ Artemis broker one can configure that directly.
To do so requires editing the `broker.xml` file found in the `etc/jms-bridge` directory.
The `broker.xml` file must be located next to the configured `jms-bridge.properties` file or it will not be found.

A default `broker.xml` is supplied with the installation.
Feel free to update it as desired.
The reference for that file can be found on the Artemis documentation site:
https://activemq.apache.org/components/artemis/documentation/latest/configuration-index.html

## Configuration Reference

Currently there are no options available to be configured via the properties file.