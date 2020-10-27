/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq;

import io.confluent.amq.config.BridgeConfig;
import io.confluent.amq.exchange.KafkaExchangeManager;
import io.confluent.amq.persistence.kafka.KafkaIntegration;
import org.apache.activemq.artemis.core.server.ActiveMQServer;

public interface ConfluentAmqServer extends ActiveMQServer {

  KafkaExchangeManager getKafkaExchangeManager();

  KafkaIntegration getKafkaIntegration();

  BridgeConfig getBridgeConfig();

}
