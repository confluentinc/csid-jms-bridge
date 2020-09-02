# JMS Bridge Installation Guide
This guide provides instructions for installing the JMS Bridge, including The JMS Bridge, and the Confluent Platform. 

For non-production enviroments (such as testing and proof-of-concept use cases), see The [quickstart guide](http://wwww.google.com) for limited installation procedures.

This guide includes the following sections:
* [Before You Install](#prerequisites)
* [Installing the JMS Bridge](###prerequisites)
* [After Installation](http://wwww.google.com)
* [Troubleshooting Installation Problems](http://wwww.google.com)
* [Uninstalling the JMS Bridge](http://wwww.google.com)


## System Requirements
Before you install the JMS Bridg:
* Review the JMS Bridge Requirements and supported OS Versions.
* Review the JMS Bridge Release Notes.

#### Hardware
The following machine recommendations are for installing individual Confluent Platform components:

+------------------------+------------+----------+----------+
| Component   | Storage   | Memory | CPU |
+========================+============+==========+==========+
| JMS Master Broker   | column 2   | column 3 | column 4 |
+------------------------+------------+----------+----------+
| JMS Slave           | ...        | ...      |          |
+------------------------+------------+----------+----------+

#### Operating Systems
The JMS Bridge required a linux distribution in order to function. Windows is not currently supported for the JMS Bridge.

+------------------------+------------+----------+----------+
| Operating System   | Early Release   | 1.0 | 2.0 |
+========================+============+==========+==========+
| RHEL/CentOS 7.x   | Yes  | Yes | Yes |
+------------------------+------------+----------+----------+

* macOS 10.13 and later is supported for testing and development purposes only.

## Before you Install 
Before you install the JMS Bridg:
* Review the JMS Bridge Requirements and supported OS Versions.
* Review the JMS Bridge Release Notes.

### Active Confluent Cluster
'ToDo'

### JDK
The JMS Bridge Requires the use of a supported JDK to operate. Current supported JDK Versions include:

+-------------+------------------+
| Component   | Version          | 
+=============+==================+
| Oracle JDK  | 1.8 or greater   | 
+-------------+------------------+

### OS Swappiness
* Talk with John about this..



#### Installing customer provided JDK
>Note: A Java optimization called compressed oops (ordinary object pointers) enables a 64-bit JVM to address heap sizes up to about 32 GB using 4-byte pointers. For larger heap sizes, 8-byte pointers are required. This means that a heap size slightly less than 32 GB can hold more objects than a heap size slightly more than 32 GB.

The Oracle JDK installer is available both as an RPM-based installer for RPM-based systems, and as a .tar.gz file. These instructions are for the .tar.gz file.
1. Download the .tar.gz file for one of the 64-bit supported versions of the Oracle JDK from [Java SE 8 Downloads](https://www.oracle.com/java/technologies/javase/javase8-archive-downloads.html). (This link is correct at the time of writing, but can change.)

    > Note:
    >
    > If you want to download the JDK directly using a utility such as wget, you must accept the Oracle license by configuring headers, which are updated frequently. Blog posts and Q&A sites can be a good source of information on how to download a particular JDK version using wget.

2. Extract the JDK to `/usr/java/jdk-version`. For Example:
    ```console
    $ tar xvfz /path/to/jdk-8u<update_version>-linux-x64.tar.gz -C /usr/java/
    ```
3. Set JAVA_HOME

    ```console
    $ touch ~/.profile
    $ vi ~/.profile
    JAVA_HOME=/usr/java/jdk1.8.0_261.jdk/Contents/Home
        export JAVA_HOME;
        $JAVA_HOME/bin/java -version
    ```
   
### Disable SELinux
Security-Enhanced Linux (SELinux) allows you to set access control through policies. If you are having trouble deploying the JMS Bridge with your policies, set SELinux in permissive mode on each host before you deploy the JMS Bridge on your node.

To set the SELinux mode, perform the following steps on each host.

1. Check the SELinux state:
    ```console
    $ getenforce
    ```
2. If the output is either 'Permissive' or 'Disabled', you can skip this task and continue on to Disabling the Firewall. If the output is enforcing, continue to the next step.
3. Open the '/etc/selinux/config' file (in some systems, the '/etc/sysconfig/selinux' file).
4. Change the line 'SELINUX=enforcing' to 'SELINUX=permissive'.
5. Save and close the file.
6. Restart your system or run the following command to disable SELinux immediately:
    ```console
    $ setenforce 0
    ```
### Disable SELinux
Most Linux platforms supported by the JMS Bridge include a feature  transparent hugepages, which interacts could impact how the JMS bridges embedded ActiveMQ Artemis server stores data in memory.  

To disable Transparent Hugepages, perform the following steps:

1. To disable transparent hugepages on reboot, add the following commands to the /etc/rc.d/rc.local file on the host:
    ```console
    echo never > /sys/kernel/mm/transparent_hugepage/enabled
    echo never > /sys/kernel/mm/transparent_hugepage/defrag
    ```

### Required Ports

### ***Optional*** System Service Account
Creating an ‘JMS’ User and Group
    ```console
    $ sudo groupadd artemis
    $ sudo useradd -s /bin/false -g artemis -d /opt/artemis artemis
    ```
### ***Optional*** System Service Account
Creating an ‘JMS’ User and Group
    ```console
    $ sudo groupadd artemis
    $ sudo useradd -s /bin/false -g artemis -d /opt/artemis artemis
    ```


### ***Optional*** Configure Network Names
Configure each host in the cluster as follows to ensure that all members can communicate with each other:

1. Set the hostname to a unique name (not localhost).
    ```console
    $ sudo hostnamectl set-hostname foo-1.example.com
    ```
2. Edit /etc/hosts with the IP address and fully qualified domain name (FQDN) of each host in the cluster. You can add the unqualified name as well.
    ```console
    1.1.1.1  foo-1.example.com  foo-1
    2.2.2.2  foo-2.example.com  foo-2
    3.3.3.3  foo-3.example.com  foo-3
    4.4.4.4  foo-4.example.com  foo-4
    ```
3. Edit '/etc/sysconfig/network' with the FQDN of this host only:
    ```console
    HOSTNAME=foo-1.example.com
    ```
4. Verify that each host consistently identifies to the network:
    1. Run 'uname -a' and check that the hostname matches the output of the hostname command.
        ```console
        HOSTNAME=foo-1.example.com
        ```
    2. Run /sbin/ifconfig and note the value of inet addr in the eth0 (or bond0) entry, for example:
    
        ```console
        eth0      Link encap:Ethernet  HWaddr 00:0C:29:A4:E8:97  
                  inet addr:172.29.82.176  Bcast:172.29.87.255  Mask:255.255.248.0
        ...
        ```
    3. Run 'host -v -t A $(hostname)' and verify that the output matches the hostname command.
       The IP address should be the same as reported by ifconfig for eth0 (or bond0):
        ```console
        Trying "foo-1.example.com"
        ...
        ;; ANSWER SECTION:
        foo-1.example.com. 60 IN	A	172.29.82.176
         ```
## Installing the JMS Bridge

### Obtaining a copy of the JMS Bridge
To obtain a copy of the JMS-Bridge, please contact your Account Executive or Confluent Support.

### Unzip the JMS Bridge Archive
        ```console
        cd /usr/local
        unzip jms-bridge-1.0.0-M1.zip 
         ```

#### Place Archive in your organizations preffered deployment directory.

```The recommended location for instation is '/opt' or '/usr/local' depending on the use of a service user account for installation.```
    
```console
cd /usr/local
unzip jms-bridge-1.0.0-M1.zip 
```
#### Update the broker directory permissions
In order to execute, the JMS bridge directory permissions must be modified. It is highly recommended to never use '777' when deploying in a production environment.
```shell
chmod -R a=rx,u+w bin/
chmod -R a=r,u+w etc/ share/
```

### The JMS bridge is now ready to 

#### Starting 
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

``` note::
    Default location of runtime files of interest:

     * Log files  -> `./logs`
     * Data files -> `./data`
```
##### Systemd

It is possible to run it via systemd but I leave that as an exercise for the reader for now.

## Configuration Options

All configuration for the JMS Bridge is done via a properties file which is supplied to the `jms-bridge-server-start` script.
An example configuration can be found in `etc/jms-bridge/jms-bridge.properties`.

### JMSBridge Configuration

Since the underlying JMS engine is a customized embedded Apache ActiveMQ Artemis broker one can configure that directly.
To do so requires editing the `broker.xml` file found in the `etc/jms-bridge` directory.
The `broker.xml` file must be located next to the configured `jms-bridge.properties` file or it will not be found.

A default `broker.xml` is supplied with the installation.
Feel free to update it as desired.
The reference for that file can be found on the Artemis documentation site:
https://activemq.apache.org/components/artemis/documentation/latest/configuration-index.html

#### Configuration Caveats

#### Clusters
JMS-Bridge clusters allow groups of JMS-bridge servers to be grouped together in order to share message processing load. Each active node in the cluster is an active JMS-Bridge server which manages its own messages and handles its own connections.

The cluster is formed by each node declaring cluster connections to other nodes in the core configuration file broker.xml . 
When a node forms a cluster connection to another node, internally it creates a core bridge (as described in Core Bridges) connection between it and the other node, this is done transparently behind the scenes - you don't have to declare an explicit bridge for each node. 

Clustering for the JMS-Bridge was tested by explicitly setting host connections. This can only be done using a static list of connectors and is configured as follows:

1. Define 'cluster-connections':
    ```xml
    <cluster-connection name="my-cluster">
        <address>jms</address>
        <connector-ref>netty-connector</connector-ref>
        <retry-interval>500</retry-interval>
        <use-duplicate-detection>true</use-duplicate-detection>
        <message-load-balancing>STRICT</message-load-balancing>
        <max-hops>1</max-hops>
        <static-connectors allow-direct-connections-only="true">
        <connector-ref>server1-connector</connector-ref>
        </static-connectors>
    </cluster-connection>
    ```
2. Add the new connector defined in the 'cluster-connection' node to the connectors xml node.
    ```xml
    <connectors>
      <!-- Default Connector.  Returned to clients during broadcast and distributed around cluster.  See broadcast and discovery-groups -->
      <connector name="activemq">
        tcp://${activemq.remoting.default.host:localhost}:${activemq.remoting.default.port:61616}
      </connector>
      <connector name="server1-connector">tcp://hostname:61616</connector>
    </connectors>
    ```

> Conector names in both the cluster-connections and the connectors node, must match.


#### High Availability and Failover
The JMS-Bridge allows servers to be linked  together as live - backup groups where each live server can have 1 or more backup servers. A backup server is owned by only one live server. Backup servers are not operational until failover occurs, however 1 chosen backup, which will be in passive mode, announces its status and waits to take over the live servers work
Before failover, only the live server is serving the Apache ActiveMQ Artemis clients while the backup servers remain passive or awaiting to become a backup server. When a live server crashes or is brought down in the correct mode, the backup server currently in passive mode will become live and another backup server will become passive. If a live server restarts after a failover then it will have priority and be the next server to become live when the current live server goes down, if the current live server is configured to allow automatic failback then it will detect the live server coming back up and automatically stop.

Due to the addition of Kafka as a storage mechanism for the JMS Bridge, only one option is currently supported for high availability in contrast to traditional ActiveMQ. 

To configure High Availability, perform the following steps:

1. Cluster two or odes together using a telopolgy of the organizations choise. Please note that any topology that uses the redistribution of journals will ot be supported.
2. On the 'master' node, please set the following configuration. the configurations 'failover-on-shutdown'
    ```xml
    <ha-policy>
      <shared-store>
        <master>
          <failover-on-shutdown>true</failover-on-shutdown>
        </master>
      </shared-store>
    </ha-policy>
    ```
```Be aware that if you restart a live server while after failover has occurred then check-for-live-server must be set to true . If not the live server will restart and server the same messages that the backup has already handled causing duplicates.```
                                                                                                                                                                                                                                                                                                                             >
3. On the 'slave' node, define the preffered state of the slave JMS-Bridge Server.
    ```xml
    <ha-policy>
      <shared-store>
        <slave>
          <allow-failback>true</allow-failback>
        </slave>
      </shared-store>
    </ha-policy>
    ```