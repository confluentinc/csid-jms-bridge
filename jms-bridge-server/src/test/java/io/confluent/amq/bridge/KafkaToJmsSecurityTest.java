/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.bridge;

import io.confluent.amq.test.KafkaTestContainer;
import io.confluent.amq.test.TestSupport;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.acl.AccessControlEntry;
import org.apache.kafka.common.acl.AccessControlEntryFilter;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.acl.AclPermissionType;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.errors.TopicAuthorizationException;
import org.apache.kafka.common.resource.PatternType;
import org.apache.kafka.common.resource.ResourcePattern;
import org.apache.kafka.common.resource.ResourcePatternFilter;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.MountableFile;

@SuppressWarnings("checkstyle:ClassDataAbstractionCoupling")
public class KafkaToJmsSecurityTest {

  private static final String ADMIN_USER = "User:admin";
  private static final String ADMIN_PASS = "admin-secret";
  private static final String AlICE_USER = "User:alice";
  private static final String ALICE_PASS = "alice-secret";
  private static final String ALICE_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
          + "username=\"alice\" "
          + "password=\"alice-secret\";";
  private static final String ADMIN_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
          + "username=\"admin\" "
          + "password=\"admin-secret\";";
  private static final String BROKER_JAAS_CONFIG =
      "org.apache.kafka.common.security.plain.PlainLoginModule required "
          + "username=\"admin\" "
          + "password=\"admin-secret\" "
          + "user_admin=\"admin-secret\" "
          + "user_alice=\"alice-secret\";";
  private static final Properties BASE_PROPS = new Properties();

  static {
    BASE_PROPS.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_PLAINTEXT");
    BASE_PROPS.put(SaslConfigs.SASL_MECHANISM, "PLAIN");
    BASE_PROPS.put(SaslConfigs.SASL_JAAS_CONFIG, ADMIN_JAAS_CONFIG);
  }


  @TempDir
  @Order(100)
  public static Path tempdir;

  @RegisterExtension
  @Order(200)
  public static final KafkaTestContainer kafkaContainer = new KafkaTestContainer(
      BASE_PROPS,
      new KafkaContainer("5.4.0")
          .withEnv(
              "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
              "BROKER:SASL_PLAINTEXT,PLAINTEXT:SASL_PLAINTEXT")
          .withEnv("KAFKA_SUPER_USERS", "User:admin")
          .withEnv("KAFKA_LOG4J_LOGGERS", "kafka.authorizer.logger=INFO")
          .withEnv("KAFKA_LOG4J_ROOT_LOGLEVEL", "INFO")
          .withEnv("KAFKA_SECURITY_PROTOCOL", "SASL_PLAINTEXT")
          .withEnv("KAFKA_SASL_MECHANISM_INTER_BROKER_PROTOCOL", "PLAIN")
          .withEnv("KAFKA_SASL_ENABLED_MECHANISMS", "PLAIN")
          .withEnv("KAFKA_SASL_MECHANISM", "PLAIN")
          .withEnv("KAFKA_SASL_JAAS_CONFIG", BROKER_JAAS_CONFIG)
          .withEnv("KAFKA_LISTENER_NAME_PLAINTEXT_PLAIN_SASL_JAAS_CONFIG", BROKER_JAAS_CONFIG)
          .withEnv("KAFKA_LISTENER_NAME_BROKER_PLAIN_SASL_JAAS_CONFIG", BROKER_JAAS_CONFIG)
          .withEnv("KAFKA_AUTHORIZER_CLASS_NAME", "kafka.security.authorizer.AclAuthorizer")
          .withEnv("KAFKA_DELETE_TOPIC_ENABLE", "true")
          .withEnv("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("broker_jaas.conf"),
              "/etc/kafka/secrets/broker_jaas.conf")
          .withEnv(
              "KAFKA_OPTS",
              "-Djava.security.auth.login.config=/etc/kafka/secrets/broker_jaas.conf")

  );

  public Properties saslProps() {
    Properties props = new Properties();
    props.putAll(kafkaContainer.defaultProps());
    props.put("sasl.mechanism", "PLAIN");
    props.put("security.protocol", "SASL_PLAINTEXT");
    return props;
  }

  public AdminClient openAdminClient() {
    Properties props = saslProps();
    props.put(SaslConfigs.SASL_JAAS_CONFIG, ADMIN_JAAS_CONFIG);
    AdminClient admin = AdminClient.create(props);
    return admin;
  }

  public interface RiskyConsumer<T> {

    void accept(T t) throws Exception;
  }

  public void withAdminClient(RiskyConsumer<AdminClient> fn) {
    try (AdminClient admin = openAdminClient()) {
      fn.accept(admin);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public AclBinding createTopicReadAcl(String principal, String topic) {
    ResourcePattern resource = new ResourcePattern(
        ResourceType.TOPIC, topic, PatternType.LITERAL);
    AccessControlEntry accessControlEntry = new AccessControlEntry(
        principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
    return new AclBinding(resource, accessControlEntry);
  }

  public AclBinding createTopicWriteAcl(String principal, String topic) {
    ResourcePattern resource = new ResourcePattern(
        ResourceType.TOPIC, topic, PatternType.LITERAL);
    AccessControlEntry accessControlEntry = new AccessControlEntry(
        principal, "*", AclOperation.WRITE, AclPermissionType.ALLOW);
    return new AclBinding(resource, accessControlEntry);
  }

  public AclBinding createConsumerGroupAcl(String principal, String consumerGroup) {
    ResourcePattern resource = new ResourcePattern(
        ResourceType.GROUP, consumerGroup, PatternType.LITERAL);
    AccessControlEntry accessControlEntry = new AccessControlEntry(
        principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
    return new AclBinding(resource, accessControlEntry);
  }

  public AclBinding createDenyAllAcl(String principal, String consumerGroup) {
    ResourcePattern resource = new ResourcePattern(
        ResourceType.GROUP, consumerGroup, PatternType.LITERAL);
    AccessControlEntry accessControlEntry = new AccessControlEntry(
        principal, "*", AclOperation.READ, AclPermissionType.ALLOW);
    return new AclBinding(resource, accessControlEntry);
  }

  public void setWriteAcls(String principal, List<String> topics) {
    List<AclBinding> bindingList = topics.stream()
        .map(t -> createTopicWriteAcl(principal, t))
        .collect(Collectors.toList());

    withAdminClient(admin ->
        admin.createAcls(bindingList).all().get());
  }

  public void setReadAcls(String principal, List<String> topics) {
    List<AclBinding> bindingList = topics.stream()
        .map(t -> createTopicReadAcl(principal, t))
        .collect(Collectors.toList());

    withAdminClient(admin ->
        admin.createAcls(bindingList).all().get());
  }

  public void setBaseAcls(String principal, String consumerGroup) {
    List<AclBinding> bindingList = new ArrayList<>();
    bindingList.add(createConsumerGroupAcl(principal, consumerGroup));

    withAdminClient(admin ->
        admin.createAcls(bindingList).all().get());
  }

  @Test
  public void testConsumingFromAllTopicsAclsPresent() throws Exception {
    int topicCount = 5;
    List<String> readAccessTopics = new LinkedList<>();
    List<String> noAccessTopics = new LinkedList<>();

    for (int i = 0; i < topicCount; i++) {
      readAccessTopics.add(kafkaContainer.safeCreateTopic("read", 1));
      noAccessTopics.add(kafkaContainer.safeCreateTopic("no-read", 1));
    }

    String groupId = "junit-group";
    setBaseAcls(AlICE_USER, groupId);
    setReadAcls(AlICE_USER, readAccessTopics);
    setWriteAcls(AlICE_USER, readAccessTopics);
    Properties kafkaProps = saslProps();
    kafkaProps.put(SaslConfigs.SASL_JAAS_CONFIG, ALICE_JAAS_CONFIG);
    kafkaProps.put(ConsumerConfig.GROUP_ID_CONFIG, "junit-group");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(
        kafkaProps, new StringDeserializer(), new StringDeserializer());
        KafkaProducer<String, String> producer = new KafkaProducer<>(
            kafkaProps, new StringSerializer(), new StringSerializer());
        AdminClient admin = AdminClient.create(kafkaProps)
    ) {

      consumer.subscribe(Pattern.compile("[^_].*"));
      consumer.poll(Duration.ofMillis(100));

      kafkaContainer.listTopics().forEach(t -> TestSupport.println("Admin topic -> {}", t));
      consumer.listTopics().forEach((topic, partitions) ->
          TestSupport.println("Alice consumer topic -> {}", topic));
      consumer.assignment().forEach(tp -> TestSupport.println("" + tp));

      admin.listTopics().names().get().forEach(s ->
          TestSupport.println("Alice admin topic -> {}", s));

      withAdminClient(root -> {
        root.describeAcls(new AclBindingFilter(
            new ResourcePatternFilter(
                ResourceType.TOPIC, null, PatternType.ANY),
            new AccessControlEntryFilter(
                AlICE_USER, "*", AclOperation.WRITE, AclPermissionType.ALLOW)))
            .values().get().forEach(acl ->
            TestSupport.println("Admin alice ACL write topic -> {}", acl.pattern().name()));
      });

      producer.partitionsFor(readAccessTopics.get(0)).forEach(pi ->
          TestSupport.println("Alice producer topic(read) partitions -> {}", pi));

      try {
        producer.partitionsFor(noAccessTopics.get(0)).forEach(pi ->
            TestSupport.println("Alice producer topic(noAccess) partitions -> {}", pi));
      } catch (TopicAuthorizationException e) {
        //as expected
      }
    }
  }
}
