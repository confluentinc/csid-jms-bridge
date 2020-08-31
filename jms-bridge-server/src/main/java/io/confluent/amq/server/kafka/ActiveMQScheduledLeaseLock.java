package io.confluent.amq.server.kafka;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.artemis.core.io.IOCriticalErrorListener;
import org.apache.activemq.artemis.core.server.ActiveMQScheduledComponent;
import org.apache.activemq.artemis.utils.actors.ArtemisExecutor;
import org.jboss.logging.Logger;

/**
 * Default implementation of a {@link ScheduledLeaseLock}: see {@link ScheduledLeaseLock#of(ScheduledExecutorService, ArtemisExecutor, String, LeaseLock, long, IOCriticalErrorListener)}.
 */
final class ActiveMQScheduledLeaseLock extends ActiveMQScheduledComponent implements ScheduledLeaseLock {

    private static final Logger LOGGER = Logger.getLogger(ActiveMQScheduledLeaseLock.class);

    private final String lockName;
    private final LeaseLock lock;
    private long lastLockRenewStart;
    private final long renewPeriodMillis;
    private final IOCriticalErrorListener ioCriticalErrorListener;

    ActiveMQScheduledLeaseLock(ScheduledExecutorService scheduledExecutorService,
                               ArtemisExecutor executor,
                               String lockName,
                               LeaseLock lock,
                               long renewPeriodMillis,
                               IOCriticalErrorListener ioCriticalErrorListener) {
        super(scheduledExecutorService, executor, 0, renewPeriodMillis, TimeUnit.MILLISECONDS, false);
        if (renewPeriodMillis >= lock.expirationMillis()) {
            throw new IllegalArgumentException("renewPeriodMillis must be < lock's expirationMillis");
        }
        this.lockName = lockName;
        this.lock = lock;
        this.renewPeriodMillis = renewPeriodMillis;
        //already expired start time
        this.lastLockRenewStart = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(lock.expirationMillis());
        this.ioCriticalErrorListener = ioCriticalErrorListener;
    }

    @Override
    public long renewPeriodMillis() {
        return renewPeriodMillis;
    }

    @Override
    public LeaseLock lock() {
        return lock;
    }

    @Override
    public synchronized void start() {
        if (isStarted()) {
            return;
        }
        this.lastLockRenewStart = System.nanoTime();
        super.start();
    }

    @Override
    public synchronized void stop() {
        if (!isStarted()) {
            return;
        }
        super.stop();
    }
}