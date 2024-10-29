---
title: Installation
description: Learn how to install the JMS Bridge
slug: 3.3.6/reference/installation
---

## Prerequisites

1. Unix based operating system
2. Java JDK 1.8 or newer

## Acquire Release Archive

At this time we do not have a place to download releases of the JMS-Bridge.
Either one must manually build it or ask for a copy of it from a Confluent representative.

## Install Archive

Installing from the archive is a manual process and is only supported on unix based systems.

Once the archive is acquired you will need a place to unzip it.
I suggest `/opt` or `/usr/local`.

```shell
cd /usr/local
unzip jms-bridge-1.0.0-M1.zip
```

Once unzipped you should have a folder named `jms-bridge-<version>` and the content of that folder should look like:

```shell
jms-bridge-1.0.0-M1-SNAPSHOT $> ls
bin   etc   lib  share
```

Those directories follow the standard linux conventions for naming.

* `bin` contains executables
* `etc` contains configuration files
* `lib` contains libraris and jars useful to external processes
* `share` contains docs, libraries and jars

Update the permissions appropriately

```shell
chmod -R a=rx,u+w bin/
chmod -R a=r,u+w etc/ share/
```

## Manual Start

To start the JMS-Bridge there is the `jms-bridge-server-start` script that can be called.
One argument is required and that is the path to the `jms-bridge.properties` file, a default one can be found in
the `etc/jms-bridge/` directory.

```shell
bin/jms-bridge-server-start etc/jms-bridge/jms-bridge.properties
```

By default it will run in the foreground and can be killed with `^C`.
Alternatively you can start it in the background using the `-daemon` option.

```shell
bin/jms-bridge-server-start -daemon etc/jms-bridge/jms-bridge.properties
```

As a companion to the `jms-bridge-server-start` script there is a `jms-bridge-server-stop` script which can be used to
stop the JMS-Bridge.

```shell
bin/jms-bridge-server-stop
```

```markdown
    Default location of runtime files of interest:

     * Log files  -> `./logs`
     * Data files -> `./data`
```

## Systemd

It is possible to run it via systemd but I leave that as an exercise for the reader for now.

## Environment Variables

The environment variables listed below can be used to change the startup behavior.

### JAVA\_HOME

The path to the Java installation you'd like to use.

### JMS\_BRIDGE\_CLASSPATH

The JVM classpath used for executing the JMS-Bridge.

This is a the basis of the classpath and will be appended to by the start scripts.

### JMS\_BRIDGE\_DATA\_DIR

The directory in which any data files required for the JMS-Bridge will be written to.

Default: `./data`

### JMS\_BRIDGE\_HEAP\_OPTS

Memory options for the JMS-Bridge's JVM.

### JMS\_BRIDGE\_JVM\_PERFORMANCE\_OPTS

Performance options for the JVM.
Includes GC settings.

### JMS\_BRIDGE\_OPTS

Additional JVM options that will be passed to the JVM.

### JMS\_BRIDGE\_JMX\_OPTS

Options used for setting up the JVM's JMX interface.

### JMS\_BRIDGE\_LOG4J\_OPTS

Logging options, based on [log4j2](https://logging.apache.org/log4j/2.x/manual).

For example to specify a custom logging configuration file you can set this property to

```shell
export JMS_BRIDGE_LOG4J_OPTS="-Dlog4j2.configurationFile=file:/path/to/log4j2.xml"
```

Alternatively to modify the logging you can edit the `etc/jms-bridge/log4j2.xml` file directly.

Included with the install is an example rolling file log4j2 configuration, `log4j2-rolling.xml`.

The `log4j2-cli.xml` file configures logging for the [jms-bridge CLI](cli) command, you may
modify that as needed.

### JMX\_PORT

The port on which the JMX interface will be available on.

### LOG\_DIR

Sets the path to the directory in which log files will be written.

Default: `./logs`
