/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import org.apache.activemq.artemis.tests.integration.cluster.failover.FailoverTest;
import org.junit.runner.RunWith;

@RunWith(JmsSuiteRunner.class)
@JmsSuiteRunner.JmsSuitePackages(
    includePackages = {
        "org.apache.activemq.artemis.tests.integration.cluster.failover"
    },
    includeClasses = {
        FailoverTest.class
    },
    includeTests = {
    },
    excludeClasses = {},
    excludePackages = {},
    excludeTests = {}
)
public class FailoverTestSuiteTest {

}

