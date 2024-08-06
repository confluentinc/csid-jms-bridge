package io.confluent.amq.cli;

import com.google.common.base.Stopwatch;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.extern.slf4j.Slf4j;

import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Can be used to shutdown any component after a given duration of time.
 */
@Slf4j
public class AutoKillSwitch {

    private final CheckedRunnable shutdownTask;
    private final Duration shutdownDuration;
    private final Stopwatch stopwatch;

    private final String name;

    private ScheduledFuture<?> scheduledFuture;

    private volatile boolean isRunning = false;
    private int checkMs;

    private final ScheduledExecutorService autoKillScheduler  = Executors
            .newSingleThreadScheduledExecutor(
                    new ThreadFactoryBuilder().setDaemon(true)
                            .setNameFormat("auto-kill-scheduler-%d")
                            .build());

    public AutoKillSwitch(String name, Duration shutdownDuration, int checkMs, CheckedRunnable shutdownTask) {
        this.shutdownTask = shutdownTask;
        this.shutdownDuration = shutdownDuration;
        this.stopwatch = Stopwatch.createStarted();
        this.name = name;
        this.checkMs = checkMs;

    }
    public AutoKillSwitch(String name, Duration shutdownDuration, CheckedRunnable shutdownTask) {
        this(name, shutdownDuration, 1000 * 60, shutdownTask);
    }

    private void performCheck() {

        try {
            if (this.stopwatch.elapsed(TimeUnit.MILLISECONDS) >= shutdownDuration.toMillis()) {
                log.debug("Auto kill scheduler '{}' has been tripped", name);
                shutdownTask.run();
                scheduledFuture.cancel(true);
                isRunning = false;
            } else {
                log.debug("Auto kill schedule '{}' not triggered", name);
            }
        } catch(Throwable e) {
            log.error("Auto kill trigger failed! Will keep trying.", e);
        }
    }

    public synchronized void start() {
        if (!isRunning) {
            scheduledFuture = autoKillScheduler
                    .scheduleAtFixedRate(this::performCheck, 0, checkMs, TimeUnit.MILLISECONDS);
            isRunning = true;
        }
    }

    public boolean isRunning() {
       return isRunning;
    }

    public interface CheckedRunnable {
        void run() throws Throwable;
    }
}

