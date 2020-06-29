package io.confluent.amq;

import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;

import java.nio.file.Files;
import java.util.Properties;

public class VanillaAmqServer {
    public static void main(String ...args) throws Exception {

        try {
           EmbeddedActiveMQ embeddedAmqServer = new EmbeddedActiveMQ();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    embeddedAmqServer.stop();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }));

            embeddedAmqServer.start();

        } catch(final Exception e) {
            System.exit(-1);
        }
    }
}
