package io.psyncopate.client;

import org.apache.activemq.artemis.api.core.management.ResourceNames;
import org.apache.activemq.artemis.api.jms.ActiveMQJMSClient;
import org.apache.activemq.artemis.api.jms.management.JMSManagementHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;

import javax.jms.*;

public class GetAddressInfo {

    private static String detectDuplicateQueues( String addressName) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory("tcp://localhost:61616");
        Connection connection = connectionFactory.createQueueConnection();
        connection.start();
        // Create a session (not transacted, with auto-acknowledgement)
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");
        QueueRequestor requestor = new QueueRequestor((QueueSession) session, managementQueue);
        Message m = session.createMessage();
        JMSManagementHelper.putOperationInvocation(m, ResourceNames.ADDRESS +addressName, "getQueueNames");
        Message reply = requestor.request(m);
        boolean success = JMSManagementHelper.hasOperationSucceeded(reply);

        if (success) {
            Object[] queueNames = (Object[]) JMSManagementHelper.getResult(reply);
            if (queueNames.length > 1) {
                return "Duplicate queues found ";
            } else {
                return "Duplicate queues not found ";
            }
        }
        return "Duplicate queues not found ";

// ------------Code to retrieve the Duplicate queue name-----------------


//            // Map to group queues by their base name
            //Pattern pattern = Pattern.compile("^(.*?)(-\\d+)?$");
//            Map<String, List<String>> queueBaseNameMap = Arrays.stream(queueNames)
//                    .map(Object::toString)
//                    .collect(Collectors.groupingBy(queue -> {
//                        Matcher matcher = pattern.matcher(queue);
//                        return matcher.matches() ? matcher.group(1) : queue;  // Get base name
//                    }));
//
//            // Filter for base names that have duplicates (i.e., queues with a numeric suffix)
//            boolean duplicateFound = queueBaseNameMap.entrySet().stream()
//                    .map(entry -> {
//                        List<String> duplicates = entry.getValue().stream()
//                                .filter(queue -> {
//                                    Matcher matcher = pattern.matcher(queue);
//                                    return matcher.matches() && matcher.group(2) != null;  // Has numeric suffix
//                                })
//                                .collect(Collectors.toList());
//
//                        // Print if duplicates are found
//                        if (!duplicates.isEmpty()) {
//                            System.out.println("Duplicate base queue found: " + entry.getKey());
//                            System.out.println("Duplicate queues: " + duplicates);
//                            return true;
//                        }
//                        return false;
//                    })
//                    .reduce(false, (acc, hasDuplicates) -> acc || hasDuplicates);  // Combine results
//
//            if (!duplicateFound) {
//                System.out.println("No duplicate queues found.");
//            }
        }
}
