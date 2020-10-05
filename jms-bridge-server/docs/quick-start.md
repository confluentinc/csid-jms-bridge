# Quick Start

Assuming you have kafka avaliable at `localhost:9092` and it is not secured (no TLS or ACLs).

## Basic JMS Operations

1. acquire a release archive
1. unzip the archive into a new directory
1. change directory to the newly unzipped one
1. set JAVA_HOME to a java 1.8 JDK installation
1. make the `bin/` files executable
1. start the server as a daemon, `jms-bridge-server-start -daemon etc/jms-bridge/quick-start-basic-jms.conf`
1. open up a new shell in the current directory
1. use the `jms-bridge` command to start a consumer
1. from the new shell use the `jms-bridge` command to start publishing messages
1. once satisfied close out the consumer and producer
1. shutdown the server by executing the `jms-bridge-server-stop` script


### By Command

```shell

mkdir jms-bridge-qs
cd jms-bridge-qs
cp ~/Downloads/jms-bridge-*.zip ./
unzip jms-bridge-*
export JAVA_HOME=<java-1.8-install-dir>
chmod -R a=rx,o+w bin/
bin/jms-bridge-server-start -daemon etc/jms-bridge/quick-start-basic-jms.conf`
bin/jms-bridge jms receive --url tcp://localhost:61616 --topic quick-start
```

In a new shell from the same directory
```shell

export JAVA_HOME=<java-1.8-install-dir>
bin/jms-bridge jms send --url tcp://localhost:61616 --topic quick-start
my first message
my second message
quit
```

In the previous shell
```shell
bin/jms-bridge-server-stop
```

## Kafka JMS Integration

Assuming you have kafka avaliable at `localhost:9092` and it is not secured (no TLS or ACLs).

For this we'll need to use some of the standard Kafka tooling to manage topics and
produce/consume from them.

Assuming `kafka-topics`, `kafka-console-consumer` and `kafka-console-producer` are available.


1. acquire a release archive
1. unzip the archive into a new directory
1. change directory to the newly unzipped one
1. set JAVA_HOME to a java 1.8 JDK installation
1. make the `bin/` files executable
1. create a kafka topic called `quick-start`
1. start the server, `jms-bridge-server-start etc/jms-bridge/quick-start-kafka-integration.conf`
1. open up 4 new shells from the current directory
1. in the first shell use the `jms-bridge jms receive` command to start a consumer consuming from the `kafka.quick-start` topic
1. in the second shell start a `kafka-console-consumer` consuming from the `quick-start` topic
1. in the third shell use the `jms-bridge jms send` command to start publishing messages to the `kafka.quick-start` topic
1. now from the third shell publish some messages, you should see them showing up in the second shell
1. in the fourth shell start a `kafka-console-producer` publishing to the `quick-start` topic
1. from the fourth shell publish some messages, they should show up in the first shell
1. repeat consuming/producing as much as desired then `control-c` each process and close shells 1, 2, and 4
1. finally shutdown the server using `control-c` in the original shell (may need to do it twice)


### By Command

In original shell
```shell script
mkdir jms-bridge-qs
cd jms-bridge-qs
cp ~/Downloads/jms-bridge-*.zip ./
unzip jms-bridge-*
export JAVA_HOME=<java-1.8-install-dir>
chmod -R a=rx,o+w bin/
cd jms-bridge-*
kafka-topics --bootstrap-server localhost:9092 --create --topic quick-start --partitions 3 --replication-factor 1
bin/jms-bridge-server-start etc/jms-bridge/quick-start-kafka-integration.conf
```

Open up 4 new shells from the same directory as the original.

In shell 1
```shell script
bin/jms-bridge jms receive --topic kafka.quick-start
```

In shell 2
```shell script
kafka-console-consumer --bootstrap-server localhost:9092 --topic quick-start
```

In shell 3
```shell script
bin/jms-bridge jms send --topic kafka.quick-start
Hello Kafka
```

Observe in shell 2 that the "Hello Kafka" message appears.

In shell 4
```shell script
kafka-console-producer --broker-list localhost:9092 --topic quick-start
Hello JMS
```

Observe in shell 1 that the "Hello JMS" message appears.

Continue publishing messages from shell 3 and 4 until you are happy.

Close shells 1, 2, 3, and 4.

In the original shell send the TERM signal to the JMS-Bridge by using `control-c`.



