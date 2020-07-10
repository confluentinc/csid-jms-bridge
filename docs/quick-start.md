# Quick Start

1. acquire a release archive
1. unzip the archive into a new directory
1. change directory to the newly unzipped one
1. set JAVA_HOME to a java 1.8 JDK installation
1. make the `bin/` files executable
1. start the server as a daemon, `jms-bridge-server-start -daemon`
1. open up a new shell in the current directory
1. use the `jms-bridge` command to start a consumer
1. from the new shell use the `jms-bridge` command to start publishing messages
1. once satisfied close out the consumer and producer
1. shutdown the server by executing the `jms-bridge-server-stop` script


## By Command

```shell

mkdir jms-bridge-qs
cd jms-bridge-qs
cp ~/Downloads/jms-bridge-*.zip ./
unzip jms-bridge-*
export JAVA_HOME=<java-1.8-install-dir>
chmod -R a=rx,o+w bin/
bin/jms-bridge-server-start -daemon etc/jms-bridge.properties
bin/jms-bridge receive --url tcp://localhost:61616 --topic quick-start
```

In a new shell from the same directory
```shell

export JAVA_HOME=<java-1.8-install-dir>
bin/jms-bridge send --url tcp://localhost:61616 --topic foo
my first message
my second message
quit
```

In the previous shell
```shell
^C
bin/jms-bridge-server-stop
```

