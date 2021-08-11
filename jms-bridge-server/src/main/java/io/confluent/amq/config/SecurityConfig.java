/*
 * Copyright 2021 Confluent Inc.
 */

package io.confluent.amq.config;

import com.typesafe.config.Config;
import org.inferred.freebuilder.FreeBuilder;

import java.util.Optional;

@FreeBuilder
public interface SecurityConfig {

  /**
   * <p>
   * The security domain that will be used by JAAS and is configured via the system property
   * <code>java.security.login.config</code> which should point to a valid JAAS configuration file.
   * </p>
   *
   * <p>
   * This is a <B>REQUIRED</B> setting.
   * </p>
   *
   * @return Security domain to use
   */
  String domain();

  /**
   * <p>
   * The security domain that will be used for certificate based authentication via  JAAS.
   * It is also configured via the file pointed to by <code>java.security.login.config</code>
   * system property.
   * </p>
   *
   * <p>
   * This is an optional setting.
   * </p>
   *
   * @return Certificate security domain to use
   */
  Optional<String> certificatDomain();

  class Builder extends SecurityConfig_Builder {

    public Builder(Config securityConfig) {
      if (securityConfig.hasPath("domain")) {
        this.domain(securityConfig.getString("domain"));
      }

      if (securityConfig.hasPath("certificate-domain")) {
        this.certificatDomain(securityConfig.getString("certificate-domain"));
      }

    }
  }
}
