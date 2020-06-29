/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Cli;
import com.github.rvesse.airline.parser.ParseResult;
import com.github.rvesse.airline.parser.errors.ParseException;

import java.util.Arrays;

@Cli(name = "JMS",
        description = "Execute basic JMS commands",
        commands = { SendCommand.class, ReceiveCommand.class })
public class JmsClient {

    public static void main(String[] args) throws Exception {
        com.github.rvesse.airline.Cli<Runnable> cli = new com.github.rvesse.airline.Cli<>(JmsClient.class);

        try {
            // Parse with a result to allow us to inspect the results of parsing
            ParseResult<Runnable> result = cli.parseWithResult(args);
            if (result.wasSuccessful()) {
                // Parsed successfully, so just run the command and exit
                result.getCommand().run();
                System.exit(0);
            } else {
                // Parsing failed
                // Display errors and then the help information
                System.err.println(String.format("%d errors encountered:", result.getErrors().size()));
                int i = 1;
                for (ParseException e : result.getErrors()) {
                    System.err.println(String.format("Error %d: %s", i, e.getMessage()));
                    i++;
                }

                System.err.println();

                com.github.rvesse.airline.help.Help.<Runnable>help(cli.getMetadata(), Arrays.asList(args), System.err);
            }
        } catch (Exception e) {
            // Errors should be being collected so if anything is thrown it is unexpected
            System.err.println(String.format("Unexpected error: %s", e.getMessage()));
            com.github.rvesse.airline.help.Help.<Runnable>help(cli.getMetadata(), Arrays.asList(args), System.err);
            System.exit(1);
        }

    }


}
