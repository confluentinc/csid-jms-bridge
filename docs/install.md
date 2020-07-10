# Installation

## Prerequisites

1. Unix based operating system
1. Java JDK 1.8 or newer

## Acquire Release Archive

At this time we do not have a place to download releases of the JMS-Bridge.
Either one must manually build it or ask for a copy of it from a Confluent representative.

## Install Archive

Installing from the archive is a manual process and is only supported on linux based systems.

Once the archive is acquired you will need a place to unzip it.
I suggest `/opt` or  `/usr/local`. 

```shell
cd /usr/local
unzip jms-bridge-1.0.0-M1.zip 
```

Once unzipped you should have a folder named `jms-bridge-<version>` and the content of that folder should look like:
```shell
jms-bridge-1.0.0-M1 $> ls
bin   etc   share
```

Those directories follow the standard linux conventions for naming.
 * `bin` contains executables
 * `etc` contains configuration files
 * `share` contains libraries and jars

Update the permissions appropriately
```shell
chmod -R a=rx,u+w bin/
chmod -R a=r,u+w etc/ share/
```

## Manual Start

To start the JMS-Bridge there is the `jms-bridge-server-start` script that can be called.
One argument is required and that is the path to the `jms-bridge.properties` file, a default one can be found in the `etc/jms-bridge/` directory.
```shell
bin/jms-bridge-server-start etc/jms-bridge/jms-bridge.properties
```

By default it will run in the foreground and can be killed with `^C`.
Alternatively you can start it in the background using the `-daemon` option.
```shell
bin/jms-bridge-server-start -daemon etc/jms-bridge/jms-bridge.properties
```

As a companion to the `jms-bridge-server-start` script there is a `jms-bridge-server-stop` script which can be used to stop the JMS-Bridge.
```shell
bin/jms-bridge-server-stop
```

## Systemd

It is possible to run it via systemd but I leave that as an exercise for the reader for now.

## Environment Variables

The environment variables listed below can be used to change the startup behavior.

### JAVA_HOME

The path to the Java installation you'd like to use.

### JMS_BRIDGE_CLASSPATH

The JVM classpath used for executing the JMS-Bridge.

This is a the basis of the classpath and will be appended to by the start scripts.

### JMS_BRIDGE_HEAP_OPTS

Memory options for the JMS-Bridge's JVM.

### JMS_BRIDGE_JVM_PERFORMANCE_OPTS

Performance options for the JVM.
Includes GC settings.

### JMS_BRIDGE_OPTS

Additional JVM options that will be passed to the JVM.

### JMS_BRIDGE_JMX_OPTS

Options used for setting up the JVM's JMX interface.

### JMS_BRIDGE_LOG4J_OPTS

Logging options, based on log4j.

For example to specify a custom logging configuration file you can set this property to
```shell
export JMS_BRIDGE_LOG4J_OPTS="-Dlog4j.configuration=file:/path/to/log4j.properties"
```

Alternatively to modify the logging you can edit the `etc/jms-bridge/log4j-rolling.properties` file directly.

### JMX_PORT

The port on which the JMX interface will be available on.

### LOG_DIR

Sets the path to the directory in which log files will be written.

### 


