/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.help.Help;

public class JmsBridgeCliHelp extends Help<Runnable> implements BaseCommand {

  @Override
  public int execute() throws Exception {
    super.run();
    return 0;
  }

}
