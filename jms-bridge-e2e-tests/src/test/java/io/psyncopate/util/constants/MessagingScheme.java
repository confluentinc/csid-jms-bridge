package io.psyncopate.util.constants;

public enum MessagingScheme {
    JMS_ANYCAST(BrokerType.JMS, RoutingType.ANYCAST),
    JMS_MULTICAST(BrokerType.JMS, RoutingType.MULTICAST),
    KAFKA_TOPIC(BrokerType.KAFKA); // Kafka doesn't use routing type

    private final BrokerType brokerType;
    private final RoutingType routingType;

    // Constructor for Artemis
    MessagingScheme(BrokerType brokerType, RoutingType routingType) {
        this.brokerType = brokerType;
        this.routingType = routingType;
    }

    // Constructor for Kafka (no routing type)
    MessagingScheme(BrokerType brokerType) {
        this.brokerType = brokerType;
        this.routingType = null;
    }

    public BrokerType getBrokerType() {
        return brokerType;
    }

    public RoutingType getRoutingType() {
        return routingType;
    }


}
