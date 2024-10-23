package io.psyncopate.ckafka.staticprops;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class DeleteAllTopicsFromKafka {

    public static void main(String[] args) {
        // Set the Kafka bootstrap servers and other configurations
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        // Create an AdminClient
        try (AdminClient adminClient = AdminClient.create(properties)) {

            ListTopicsOptions listTopicsOptions = new ListTopicsOptions();
            listTopicsOptions.listInternal(true); // Include internal topics

            // Get the list of topics
            ListTopicsResult listTopicsResult = adminClient.listTopics(listTopicsOptions);
            KafkaFuture<Set<String>> topicsFuture = listTopicsResult.names();

            // Wait for the result and print the topics
            Set<String> topics = topicsFuture.get();
            System.out.println("Total topics : "+topics.size());
            topics.removeIf(topic -> topic.startsWith("__") || topic.startsWith("docker") || topic.startsWith("_confluent") || topic.startsWith("_schemas") );
            /*System.out.println("List of topics:");
            for (String topic : topics) {
                System.out.println(topic);
            }*/

            Predicate<String> filter = Pattern
                    .compile("^[a-zA-Z]*$")
                    .asPredicate();
            Set<String> filteredTopics = topics
                    .stream()
                    .filter(filter)
                    .collect(Collectors.<String>toSet());

            System.out.println("List of topics:");
            for (String topic : topics) {
                System.out.println(topic);
            }
            // List of topics to be deleted
            // You can replace "topic1", "topic2", etc., with the actual names of the topics you want to delete
            //Iterable<String> topicsToDelete = Arrays.asList("topic1", "topic2", "topic3");

            // Configure deletion options
            //DeleteTopicsOptions deleteTopicsOptions = new DeleteTopicsOptions();
            //deleteTopicsOptions.timeoutMs(5000); // Set timeout in milliseconds

            // Delete topics
            //DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(filteredTopics);
            // Wait for topics deletion to complete
            //deleteTopicsResult.all().get();
            System.out.println("Topics deleted successfully.");
        } catch (InterruptedException | ExecutionException e) {
            System.err.println("Error deleting topics: " + e.getMessage());
        }
    }
}
