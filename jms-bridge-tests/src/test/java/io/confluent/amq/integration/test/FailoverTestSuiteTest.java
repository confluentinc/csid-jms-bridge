/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import io.confluent.amq.JmsBridgeConfiguration;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.artemis.core.config.Configuration;
import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTest;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.ClassRule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.testcontainers.containers.KafkaContainer;

@RunWith(JmsSuiteRunner.class)
@JmsSuiteRunner.JmsSuitePackages(
    includePackages = {
        "org.apache.activemq.artemis.tests.integration.jms",
        "org.apache.activemq.artemis.tests.integration.cluster.failover"
    },
    includeClasses = {
        FailoverTest.class
    },
    includeTests = {
        "FailoverTest#testTransactedMessagesSentSoRollbackAndContinueWork"
    },
    excludeClasses = {},
    excludePackages = {},
    excludeTests = {}
)
public class FailoverTestSuiteTest {

}

