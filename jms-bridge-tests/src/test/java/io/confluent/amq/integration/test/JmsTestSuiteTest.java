/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.integration.test;

import org.apache.activemq.artemis.tests.integration.jms.RedeployTempTest;
import org.apache.activemq.artemis.tests.integration.jms.cluster.BindingsClusterTest;
import org.apache.activemq.artemis.tests.integration.jms.largemessage.JMSLargeMessageTest;
import org.junit.runner.RunWith;

@RunWith(JmsSuiteRunner.class)
@JmsSuiteRunner.JmsSuitePackages(
    includePackages = {
        "org.apache.activemq.artemis.tests.integration.jms"
    },
    includeClasses = {
    },
    excludeTests = {
        // These tests do not work with vanilla AMQ
        "SimpleJNDIClientTest#testRemoteCFWithUDP",
        "SimpleJNDIClientTest#testConnectionFactoryStringWithInvalidParameter",
        "ActiveMQConnectionFactoryTest#testDiscoveryConstructor",
        "ManualReconnectionToSingleServerTest#testExceptionListener",

        //These are being excluded because they test features not supported by the kafka backend
        //--------------------------------------------------------------------------------------
        //--The below require large message support
        "JMSLargeMessageTest#testResendWithLargeMessage",
        "TextMessageTest#testSendReceiveWithBody0xffffplus1",
        "TextMessageTest#testSendReceiveWithBody0xfffftimes2",
        "TextMessageTest#testSendReceiveWithBody0xffff",
        "ReSendMessageTest#testResendWithLargeMessage"
    },
    excludeClasses = {
        RedeployTempTest.class,
        JMSLargeMessageTest.class
    },
    excludePackages = {
    },
    includeTests = {
    }
)
public class JmsTestSuiteTest {

}
