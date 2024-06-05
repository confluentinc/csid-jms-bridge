///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.7.5
//DEPS org.apache.activemq:artemis-jms-client-all:2.33.0
//DEPS org.slf4j:slf4j-api:2.0.13
//DEPS org.slf4j:slf4j-simple:2.0.13
//JAVA 11

import picocli.CommandLine;
import picocli.CommandLine.Command;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "JMS Producer", mixinStandardHelpOptions = true, version = "Producer 0.1",
        description = "Send JMS Messages.")
class Producer implements Callable<Integer> {

    @CommandLine.Option(names = {"-t", "--topic"}, description = "The topic to send the text message to", required = true)
    private String topic;
    @CommandLine.Option(names = {"-d", "--destination"}, description = "Where to produce messages to", defaultValue = "(tcp://localhost:61617,tcp://localhost:61616)?ha=true&retryInterval=100&retryIntervalMultiplier=1.0&reconnectAttempts=-1&failoverOnServerShutdown=true;", required = true)
    private String destination;

    @CommandLine.Parameters
    private String message;


    public static void main(String... args) {
        int exitCode = new CommandLine(new Producer()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
        properties.put("connectionFactory.ConnectionFactory", destination);

        InitialContext initialContext = new InitialContext(properties);
        ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
        try (Session session = cf.createConnection().createSession()) {
            Topic requestTopic = session.createTopic("kafka." + topic);
            TopicSession topicSession = (TopicSession) session;
            MessageProducer producer = session.createProducer(requestTopic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);

            var numMessages = 0;
            while (true) {
                try {
                    numMessages++;
                    System.out.println(String.format("Sending message: %s %d ", message, numMessages));
                    TextMessage tmsg = topicSession.createTextMessage(String.format("%s %d", message, numMessages));
                    producer.send(tmsg);
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    System.out.println("Interrupted!");
                    break;
                } catch (JMSException e) {
                    System.out.println(e.getMessage());
                    break;
                }
            }
        }
        return 0;
    }
}
