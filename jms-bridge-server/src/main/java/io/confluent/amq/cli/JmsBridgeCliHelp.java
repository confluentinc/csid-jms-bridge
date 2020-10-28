/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.help.Help;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

public class JmsBridgeCliHelp extends Help<Runnable> implements BaseCommand {

  public JmsBridgeCliHelp() {
    super();
  }

  @Override
  public int execute(CommandIo io) throws Exception {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    help(global, command, includeHidden, baos);

    if (baos.size() == 0) {
      io.output().println("See the -h or --help flags for usage information");
    } else {
      io.output().print(new String(baos.toByteArray(), StandardCharsets.UTF_8));
    }
    return 0;
  }

}
