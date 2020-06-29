/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.help.Help;

@Cli(name = "jms-bridge",
    description = "Command line client to the jms-bridge",
    defaultCommand = Help.class,
    commands = {})
public class JmsBridge {

}
