/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Optional;
import org.inferred.freebuilder.FreeBuilder;

@FreeBuilder
public interface RoutingConfig {

  Optional<String> deadLetterTopic();

  List<Route> routes();

  class Builder extends RoutingConfig_Builder {

    public Builder() {

    }

    public Builder(Config routingConfig) {
      if (routingConfig.hasPath("dead-letter-topic")) {
        this.deadLetterTopic(routingConfig.getString("dead-letter-topic"));
      }

      if (routingConfig.hasPath("routes")) {
        for (Config routeConfig: routingConfig.getConfigList("routes")) {
          Config defRouteConfig = routeConfig.withFallback(
              ConfigFactory.defaultReference().getConfig("default-route"));
          this.addRoutes(new Route.Builder(defRouteConfig));
        }
      }
    }
  }

  @FreeBuilder
  interface Route {

    String name();

    In from();

    Convert map();

    Out to();


    class Builder extends RoutingConfig_Route_Builder {

      public Builder() {

      }

      public Builder(Config config) {
        this.name(config.getString("name"))
            .from(new In.Builder(config.getConfig("from")))
            .map(new Convert.Builder(config.getConfig("map")))
            .to(new Out.Builder(config.getConfig("to")));
      }

    }
  }

  @FreeBuilder
  interface In {

    String include();

    class Builder extends RoutingConfig_In_Builder {

      public Builder() {

      }

      public Builder(Config config) {
        this.include(config.getString("include"));
      }

    }
  }

  @FreeBuilder
  interface Out {

    String topic();

    class Builder extends RoutingConfig_Out_Builder {

      public Builder() {

      }

      public Builder(Config config) {
        this.topic(config.getString("topic"));
      }

    }
  }

  @FreeBuilder
  interface Convert {

    String key();

    class Builder extends RoutingConfig_Convert_Builder {

      public Builder() {

      }

      public Builder(Config config) {
        this.key(config.getString("key"));
      }

    }
  }
}
