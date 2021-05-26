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

### Logging

#### Changing Specific Logger Levels

Logging can be configured via environmental variables in the following way.

To set the logging level for a paticular logger (say `io.confluent`) set the corresponding environment variable.
```LOG4J2_IO_CONFLUENT=INFO```

The prefix `LOG4J2_` combined with the logger name (capatilized with dots replaced by underscores).

#### Root Logger Level

To change this from the default of `WARN` set the environmental variable `LOG4J2ROOT` to the level desired.
```LOG4J2ROOT=INFO```




