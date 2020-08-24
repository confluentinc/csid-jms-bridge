/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import static java.lang.String.format;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.concurrent.CompletableFuture;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@ExtendWith(CapturedCommandIo.class)
public class SendCommandTest {

  @Mock
  private Session session;

  @Mock
  private MessageProducer producer;

  @Mock
  private Topic topic;

  private SendCommand sendCommand;

  @BeforeEach
  public void beforeEach(CapturedCommandIo commandIo) {
    sendCommand = new SendCommand();
  }

  public CompletableFuture<Void> goodSetup(CapturedCommandIo commandIo) throws Exception {
    sendCommand.topic = "my-topic";
    Mockito.when(session.createTopic(sendCommand.topic)).thenReturn(topic);
    Mockito.when(session.createProducer(topic)).thenReturn(producer);

    final CompletableFuture<Void> cmd = CompletableFuture.runAsync(
        runner(() -> sendCommand.send(commandIo).withSession(session)));

    retry(5000, 100, () -> assertTrue(commandIo.outputReader().ready()));

    return cmd;
  }

  @Test
  public void sendQuit(CapturedCommandIo commandIo) throws Exception {
    final CompletableFuture<Void> cmd = goodSetup(commandIo);

    commandIo.inputWriter().write("quit");
    commandIo.inputWriter().newLine();
    commandIo.inputWriter().flush();

    retry(5000, 100, () -> {
      assertTrue(cmd.isDone());
    });

    Mockito.verify(producer).setDeliveryMode(DeliveryMode.PERSISTENT);
    Mockito.verifyNoMoreInteractions(session, topic, producer);
  }

  @Test
  public void sendMessage(CapturedCommandIo commandIo) throws Exception {
    final CompletableFuture<Void> cmd = goodSetup(commandIo);

    commandIo.inputWriter().write("my message");
    commandIo.inputWriter().newLine();
    commandIo.inputWriter().flush();

    commandIo.inputWriter().write("quit");
    commandIo.inputWriter().newLine();
    commandIo.inputWriter().flush();

    retry(5000, 100, () -> {
      assertTrue(cmd.isDone());
    });

    Mockito.verify(session, Mockito.times(1)).createTextMessage("my message");
    Mockito.verify(producer, Mockito.times(1)).send(Mockito.any());
  }

  public void retry(long maxTimeMillis, long delayMillis, CarelessRunner runner)
      throws Exception {

    StopWatch stopWatch = StopWatch.createStarted();
    int failCount = 0;
    AssertionError lastError = null;
    while (stopWatch.getTime() < maxTimeMillis) {
      try {
        runner.run();
        return;
      } catch (AssertionError failed) {
        failCount++;
        lastError = failed;
        Thread.sleep(delayMillis);
      }
    }
    stopWatch.stop();

    fail(format(
        "Assertion failed after %s attempts over a period of %s milliseconds", failCount,
        stopWatch.getTime()), lastError);
  }

  public Runnable runner(CarelessRunner runner) {
    return () -> {
      try {
        runner.run();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    };
  }

  interface CarelessRunner {

    void run() throws Exception;
  }

}