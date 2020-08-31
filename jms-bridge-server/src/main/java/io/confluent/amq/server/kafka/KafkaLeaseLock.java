package io.confluent.amq.server.kafka;

final class KafkaLeaseLock  implements LeaseLock{
    @Override
    public boolean tryAcquire() {
        return false;
    }

    @Override
    public boolean isHeld() {
        return false;
    }

    @Override
    public boolean isHeldByCaller() {
        return false;
    }

    @Override
    public void release() {

    }
}