/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.jms.bridge.itests;

import com.google.common.io.Resources;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Contains constants and resources required to execute integration tests.
 */
public final class TestResources {
    private TestResources() {
    }

    @SuppressWarnings({"UnstableApiUsage"})
    public static String jmsBridgeDockerImageString() {
        try {
            return Resources.toString(
                    Resources.getResource("META-INF/docker/com.spotify/foobar/image-name"),
                    StandardCharsets.UTF_8);
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String kafkaDockerImageString() {
        return "confluentinc/cp-kafka:6.2.0-3-ubi8";
    }


    public static final DockerImageName kafkaDocker() {
        return DockerImageName.parse(kafkaDockerImageString());
    }

    public static final DockerImageName jmsBridgeDocker() {
        return DockerImageName
                .parse(jmsBridgeDockerImageString());
    }
}
