/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Properties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Disabled("Work in Progress")
@ExtendWith(MockitoExtension.class)
public class JmsBridgeMainTest {
  @Mock
  ConfluentEmbeddedAmq mockAmq;
  JmsBridgeMain subject;

  @SuppressFBWarnings("URF_UNREAD_FIELD")
  @BeforeEach
  public void setup() {
    subject = new JmsBridgeMainSpy(mockAmq);
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