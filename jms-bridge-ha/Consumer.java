///usr/bin/env jbang "$0" "$@" ; exit $?
//DEPS info.picocli:picocli:4.7.5
//DEPS org.apache.activemq:artemis-jms-client-all:2.13.0
//JAVA 11

import picocli.CommandLine;
import picocli.CommandLine.Command;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Callable;

@Command(name = "JMS Consumer", mixinStandardHelpOptions = true, version = "Consumer 0.1",
        description = "Send JMS Messages.")
class Consumer implements Callable<Integer> {

    @CommandLine.Option(names = {"-t", "--topic"}, description = "The topic to receive text messages from", required = true)
    private String topic;

    @CommandLine.Option(names = {"-s", "--source"}, description = "Where to consume messages from", defaultValue = "(tcp://localhost:61617?name=live-netty-connector,tcp://localhost:61616?name=backup-netty-connector)?ha=true&reconnectAttempts=-1&failoverAttempts=-1;", required = true)
    private String source;


    public static void main(String... args) {
        int exitCode = new CommandLine(new Consumer()).setCaseInsensitiveEnumValuesAllowed(true).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() throws Exception {
        Properties properties = new Properties();
        properties.put("java.naming.factory.initial", "org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory");
        properties.put("connectionFactory.ConnectionFactory", source);

        InitialContext initialContext = new InitialContext(properties);
        ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");
        try (Connection connection = cf.createConnection()) {
            connection.setClientID("consumer-" + UUID.randomUUID());
            connection.start();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue consumeTopic = session.createQueue("KafkaTest");
            MessageConsumer consumer = session.createConsumer(consumeTopic);

            while (true) {
                try {
                    Message message = consumer.receive();
                    System.out.println(String.format("Received message: %s", ((TextMessage) message).getText()));
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
