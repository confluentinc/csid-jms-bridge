/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

public interface BaseCommand {

  /**
   * <p>
   * Execute command and returns back an exit code that will be used.
   * </p>
   * <p>
   * If an exception occurs then an exit code of <code>1</code> will be used.
   * </p>
   * @return system exit code
   */
  int execute() throws Exception;

}
