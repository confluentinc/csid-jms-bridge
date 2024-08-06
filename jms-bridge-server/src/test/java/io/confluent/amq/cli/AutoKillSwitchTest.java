package io.confluent.amq.cli;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.*;

class AutoKillSwitchTest {


    @Test
    public void testKillTime() throws Exception {
        CompletableFuture<?> future = new CompletableFuture<>();
        var subject = new AutoKillSwitch("test kill switch", Duration.ofMillis(100), 10, () -> {
            future.complete(null);
        });
        subject.start();

        try {
            future.get(1, TimeUnit.SECONDS);
        } catch (Exception e) {
            fail();
        }
    }
}