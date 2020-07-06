/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import static org.junit.Assert.*;

import java.util.Properties;
import org.apache.activemq.artemis.core.server.embedded.EmbeddedActiveMQ;
import org.junit.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class JmsBridgeMainTest {
  @Mock
  ConfluentEmbeddedAmq mockAmq;
  JmsBridgeMain subject = new JmsBridgeMainSpy(mockAmq);

  @Test
  public void noBrokerXml() {
    subject.ex

  }

  public static class JmsBridgeMainSpy extends JmsBridgeMain {
    final ConfluentEmbeddedAmq mockAmq;

    public JmsBridgeMainSpy(ConfluentEmbeddedAmq mockAmq) {
      this.mockAmq = mockAmq;
    }

    @Override
    protected ConfluentEmbeddedAmq loadServer(Properties serverProps,
        String brokerXmlPath) {
      return mockAmq;
    }
  }
}