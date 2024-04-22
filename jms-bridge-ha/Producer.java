///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.7.5
//DEPS org.apache.activemq:artemis-jms-client-all:2.13.0


import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Properties;
import java.util.concurrent.Callable;

@Command(name = "JMS Producer", mixinStandardHelpOptions = true, version = "Producer 0.1",
        description = "Send JMS Messages.")
class Producer implements Callable<Integer> {

    @CommandLine.Option(names = {"-t", "--topic"}, description = "The topic to send the text message to", required = true)
    private String topic;

    @CommandLine.Option(names = {"-d", "--destination"}, description = "Where to produce messages to", defaultValue = "tcp://localhost:61616", required = true)
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
        properties.put("topic." + topic, topic);

        InitialContext initialContext = new InitialContext(properties);
        ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
        try (Session session = cf.createConnection().createSession()) {
            Topic requestTopic = session.createTopic(topic);
            TopicSession topicSession = (TopicSession) session;
            TopicRequestor requestor = new TopicRequestor(topicSession, requestTopic);

            var numMessages = 0;
            while (true) {
                try {
                    numMessages++;
                    System.out.println(String.format("Sending message: %s %d ", message, numMessages));
                    TextMessage tmsg = topicSession.createTextMessage(String.format("%s %d", message, numMessages));
                    Message response = requestor.request(tmsg);
                    System.out.println("Received response: " + response);
                    Thread.sleep(1000);
                } catch (InterruptedException interruptedException) {
                    System.out.println("Interrupted!");
                } catch (JMSException e) {
                    e.printStackTrace();
                } finally {
                    System.out.println("Closing connection");
                    break;
                }
            }
        }
        return 0;
    }
}
