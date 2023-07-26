# jms-bridge-docker

Module for building JMS Bridge docker images.

## To build locally

To build a new image with local changes:

1. Ensure you're logged in to docker:
    ```
    > docker login
    ```

1. Build docker images from local changes.
    ```
    > mvn clean
    > mvn -Pdocker package -DskipTests -Dspotbugs.skip -Dcheckstyle.skip  -Ddockerfile.skip=false -Dskip.docker.build=false -Ddocker.upstream-tag=latest-ubi8 -Ddocker.tag=local.build  -Ddocker.upstream-registry=''
    ```
   Change `docker.upstream-tag` if you want to depend on anything other than the latest master upstream, e.g. 5.4.x-latest.

1. Check the image was built:
    ```
    > docker image ls | grep local.build
    ```
    You should see the new image listed. For example:

    ```
    placeholder/confluentinc/jms-bridge-docker       local.build   94210cd14384   About an hour ago   716MB
    ```
   
## Configuration

Several environment variables can be used to modify the configuration of the container.

**BROKER_XML**

*Default*: `/etc/jms-bridge/broker.xml`

Set this to a valid path within the container that resolves to the `broker.xml` that Artemis should use.

**PROMETHEUS_ENABLED**

*Unset By Default*

Set this to y/Y/t/T/1 to enable the JMX prometheus agent for exposing JMX metrics as a prometheus endpoint.

When enabled the following variables can be used to configure the prometheus agent

 * **PROMETHEUS_PORT**: *Default `8080`*, Set the port the prometheus endpoint should bind to.
 * **PROMETHEUS_CONF**: *Default `/etc/jms-bridge/jmx-prometheus-config.yml`*, The path to the configuration to be used for the agent.

### jms-bridge.conf

All elements of the `jms-bridge.conf` file can be modified via environment variables using the `JMSBRIDGE_` prefix.

**Prefix JMSBRIDGE_**

Use the prefix `JMSBRIDGE_` combined with the config path (capitilized with dots replaced by underscores) to make modifications to arbitrary parts of the configuration.
 
Examples:

 * Add/Modify the `bridge.id`: `JMSBRIDGE_ID=my-bridge-id`
 * Add/Modify the first routing entries match `bridge.routing.topics.0.match`: `JMSBRIDGE_ROUTING_TOPICS_0_MATCH=quick-start-.*`

### Logging

Logging can be adjusted via environmental variables.

#### Changing Specific Logger Levels

Logging can be configured via environmental variables in the following way.

To set the logging level for a paticular logger (say `io.confluent`) set the corresponding environment variable.
```LOG_IO_CONFLUENT=INFO```

The prefix `LOG_` combined with the logger name (capitilized with dots replaced by underscores).

#### Root Logger Level

To change this from the default of `WARN` set the environmental variable `LOGROOT` to the level desired.
```LOGROOT=INFO```




