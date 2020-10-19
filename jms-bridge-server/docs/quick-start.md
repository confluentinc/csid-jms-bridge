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


## Request / Reply Pattern

In JMS there two different messaging style supported, publish-subscribe (pubsub) and point-to-point (ptp).
This differs from Kafka which only supports the pubsub model.
Since the JMS Bridge resides in both worlds it can be used to facilitate a ptp like interaction between JMS clients and Kafka clients.

In this example a JMS client will be performing a synchronous request expecting a reply while the Kafka client will be aysnchronously responding to it.
From the JMS client's point of view nothing is unusual since it already supports the ptp model.
The Kafka client, on the other hand, will need to do a little extra work to tie the request to the response.

This example is done in Java and will require the reader to know enough about java development to finish the code, compile it and then execute it.

Here's an example main method for the JMS client:
```java

    try (
        //acquire a JMS Session
        Session session = amqServer.getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE)
    ) {

      // The JMS topic connected to the Kafka topic that our Kafka client will be responding from
      Topic requestAmqTopic = session.createTopic("kafka.quick-start-request");

      TopicSession topicSession = (TopicSession) session;
      TopicRequestor requestor = new TopicRequestor(topicSession, requestAmqTopic);

      try {
        String request = "Hello, what's your name?";
        TextMessage tmsg = session.createTextMessage(request);
        System.out.println("Request: " + request);
        Message response = requestor.request(tmsg);
        System.out.println("Response: " + response.getBody(String.class));
      } catch (RuntimeException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }

      System.out.println("jms disconnected.");
    } 
  }
```

Here for the Kafka client:
```java
      
      try (
          KafkaProducer<byte[], String> kproducer = new KafkaProducer<>(
             kprops, new ByteArraySerializer(), new StringSerializer());
          KafkaConsumer<byte[], String> kconsumer = new KafkaConsumer<>(
             kprops, new ByteArrayDeserializer(), new StringDeserializer());
      ) {

        kconsumer.subscribe(Collections.singleton("quick-start-request"));

        while (true) {
          ConsumerRecords<byte[], String> pollRecords = kconsumer.poll(Duration.ofMillis(100L));
          if (pollRecords != null) {
            pollRecords.forEach(request -> {

              //Extract the JMSReployTo header value, should refer to a temporary topic
              Header replyTo = request.headers().lastHeader("jms.JMSReplyTo");
              final byte[] destination = replyTo != null
                  ? replyTo.value()
                  : null;

              String responseValue = "Hi, my name is Kafka";
              System.out.println(" Response: " + responseValue);

              ProducerRecord<byte[], String> response = new ProducerRecord(
                "quick-start-response", request.key(), responseValue);

              if (destination != null) {
                //set the destination to that temporary topic from the request header
                response.headers().add("jms.JMSDestination", destination);
              }

              //synchronous publish for example only, not recommended, use async version with callback instead
              kproducer.send(response).get();
            });
          }
        }
      }
```

For the example to work some preparatory tasks need to be completed.

1. Create the `quick-start-request` and `quick-start-response` topics in Kafka.
2. Start the JMS Bridge using the `etc/jms-bridge/quick-start-kafka-request-reply.conf` configuration with the correct `bootstrap.servers`.



