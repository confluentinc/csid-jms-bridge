package io.psyncopate.client.Model;

public class QueueAttributes {
    private boolean durable;
    private int consumerCount;
    private int messageCount;
    private int maxConsumers;
    private int scheduledCount;
    private int deliveringCount;
    private boolean paused;
    private boolean temporary;
    private String routingType;
    private boolean autoDeleted;
    private String name;
    private long created;
    private long lastMessageTimestamp;
    private long messageExpiration;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public long getCreated() {
        return created;
    }

    public void setCreated(long created) {
        this.created = created;
    }

    public long getLastMessageTimestamp() {
        return lastMessageTimestamp;
    }

    public void setLastMessageTimestamp(long lastMessageTimestamp) {
        this.lastMessageTimestamp = lastMessageTimestamp;
    }

    public long getMessageExpiration() {
        return messageExpiration;
    }

    public void setMessageExpiration(long messageExpiration) {
        this.messageExpiration = messageExpiration;
    }

    // Getters and Setters
    public boolean isDurable() { return durable; }
    public void setDurable(boolean durable) { this.durable = durable; }

    public int getConsumerCount() { return consumerCount; }
    public void setConsumerCount(int consumerCount) { this.consumerCount = consumerCount; }

    public int getMessageCount() { return messageCount; }
    public void setMessageCount(int messageCount) { this.messageCount = messageCount; }

    public int getMaxConsumers() { return maxConsumers; }
    public void setMaxConsumers(int maxConsumers) { this.maxConsumers = maxConsumers; }

    public int getScheduledCount() { return scheduledCount; }
    public void setScheduledCount(int scheduledCount) { this.scheduledCount = scheduledCount; }

    public int getDeliveringCount() { return deliveringCount; }
    public void setDeliveringCount(int deliveringCount) { this.deliveringCount = deliveringCount; }

    public boolean isPaused() { return paused; }
    public void setPaused(boolean paused) { this.paused = paused; }

    public boolean isTemporary() { return temporary; }
    public void setTemporary(boolean temporary) { this.temporary = temporary; }

    public String getRoutingType() { return routingType; }
    public void setRoutingType(String routingType) { this.routingType = routingType; }

    public boolean isAutoDeleted() { return autoDeleted; }
    public void setAutoDeleted(boolean autoDeleted) { this.autoDeleted = autoDeleted; }
}

