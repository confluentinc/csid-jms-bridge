/*
 * Copyright 2020 Confluent Inc.
 */
package io.confluent.amq;

import com.github.rvesse.airline.SingleCommand;
import com.github.rvesse.airline.annotations.Arguments;
import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import com.github.rvesse.airline.help.Help;
import com.github.rvesse.airline.parser.errors.ParseException;

import java.io.File;
import java.io.IOException;

@Command(name="server", description="JMS Bridge Server")
public class ServerOptions {

    @SuppressWarnings("unused") // Accessed via reflection
    @Once
    @Required
    @Arguments(
            title = "config-file",
            description = "A file specifying configs for the JMS-Bridge Server.")
    private String propertiesFile;

    public File getPropertiesFile() {
        return new File(propertiesFile);
    }

    public static ServerOptions parse(final String...args) throws IOException {
        final SingleCommand<ServerOptions> optionsParser = SingleCommand.singleCommand(ServerOptions.class);

        // If just a help flag is given, an exception will be thrown due to missing required options;
        // hence, this workaround
        for (final String arg : args) {
            if ("--help".equals(arg) || "-h".equals(arg)) {
                Help.help(optionsParser.getCommandMetadata());
                return null;
            }
        }

        try {
            return optionsParser.parse(args);
        } catch (final ParseException exception) {
            if (exception.getMessage() != null) {
                System.err.println(exception.getMessage());
            } else {
                System.err.println("Options parsing failed for an unknown reason");
            }
            System.err.println("See the -h or --help flags for usage information");
        }
        return null;
    }
}
