package io.confluent.amq.server.kafka;

import java.util.concurrent.ScheduledExecutorService;

import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.server.ActiveMQComponent;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;

/**
 * {@link LeaseLock} holder that allows to schedule a {@link LeaseLock#renew} task with a fixed {@link #renewPeriodMillis()} delay.
 */
interface ScheduledLeaseLock extends ActiveMQComponent {

    LeaseLock lock();

    long renewPeriodMillis();

    static ScheduledLeaseLock of(ScheduledExecutorService scheduledExecutorService,
                                 ArtemisExecutor executor,
                                 String lockName,
                                 LeaseLock lock,
                                 long renewPeriodMillis,
                                 IOCriticalErrorListener ioCriticalErrorListener) {
        return new ActiveMQScheduledLeaseLock(scheduledExecutorService, executor, lockName, lock, renewPeriodMillis, ioCriticalErrorListener);
    }

}