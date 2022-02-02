/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.exchange;

import org.apache.activemq.artemis.core.filter.Filter;
import org.apache.activemq.artemis.core.server.Consumer;
import org.apache.activemq.artemis.core.server.HandleStatus;
import org.apache.activemq.artemis.core.server.MessageReference;
import org.apache.activemq.artemis.core.server.Queue;

import io.confluent.amq.logging.StructuredLogger;

import java.util.Collections;
import java.util.List;


/*
A Shadow consumer that is used to immediately ack all messages destined for kafka.  The underlying
stream processor does the actual propagation of the message to Kafka.

This consumer is needed to fulfill AMQs routing requirements that allows the message to flow through
the system.
 */
public class KafkaExchangeIngress implements Consumer {

  private static final StructuredLogger SLOG = StructuredLogger
      .with(b -> b.loggerClass(KafkaExchangeIngress.class));

  private boolean active;

  private final Queue queue;

  private final long sequentialID;

  public KafkaExchangeIngress(
      Queue queue,
      long sequentialID) {

    this.queue = queue;

    this.sequentialID = sequentialID;

  }

  @Override
  public long sequentialID() {
    return sequentialID;
  }

  @Override
  public Filter getFilter() {
    return null;
  }

  @Override
  public String debug() {
    return toString();
  }

  @Override
  public String toManagementString() {
    return "KafkaExchangeIngress[" + queue.getName() + "/" + queue.getID() + "]";
  }

  @Override
  public void disconnect() {
    //noop
  }

  public synchronized void start() throws Exception {
    active = true;
    SLOG.trace(b -> b
        .event("Start")
        .putTokens("queue", queue));
    queue.addConsumer(this);
    queue.deliverAsync();
  }

  public synchronized void stop() throws Exception {
    active = false;
    SLOG.trace(b -> b
        .event("Stop")
        .putTokens("queue", queue));
    queue.removeConsumer(this);
  }

  public synchronized void close() {
    active = false;
    SLOG.trace(b -> b
        .event("Close")
        .putTokens("queue", queue));
    queue.removeConsumer(this);
  }

  @Override
  public synchronized HandleStatus handle(MessageReference reference) throws Exception {
    //Ack all messages immediately
    reference.handled();
    reference.incrementDeliveryCount();
    reference.getQueue().acknowledge(reference);
    reference.getQueue().deliverAsync();
    return HandleStatus.HANDLED;
  }

  @Override
  public void proceedDeliver(MessageReference reference) {
    // no op
  }

  /* (non-Javadoc)
   * @see org.apache.activemq.artemis.core.server.Consumer#getDeliveringMessages()
   */
  @Override
  public List<MessageReference> getDeliveringMessages() {
    return Collections.emptyList();
  }
}
