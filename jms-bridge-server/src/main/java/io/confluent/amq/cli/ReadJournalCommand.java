/*
 * Copyright 2020 Confluent Inc.
 */

package io.confluent.amq.cli;

import com.github.rvesse.airline.annotations.Command;
import com.github.rvesse.airline.annotations.Option;
import com.github.rvesse.airline.annotations.restrictions.Once;
import com.github.rvesse.airline.annotations.restrictions.Required;
import io.confluent.amq.persistence.kafka.journal.KafkaJournalDescriber;
import javax.inject.Inject;

@Command(name = "read", description = "Read an existing kafka journal from a jms bridge.")
public class ReadJournalCommand implements BaseCommand {

  @Inject
  KafkaClientOptions kafkaClientOptions = new KafkaClientOptions();

  @Option(
      name = {"-b", "--bridge.id"},
      description = "The ID of the bridge to read the journal of.")
  @Once
  @Required
  String bridgeId;

  @Option(
      name = {"-j", "--journal"},
      description = "The name of the journal to read, 'messages' or 'bindings' usually.")
  @Once
  @Required
  String journalName;

  @Override
  public int execute(CommandIo io) throws Exception {

    KafkaJournalDescriber jdesc =  new KafkaJournalDescriber(
        kafkaClientOptions.getKafkaOpts(), journalName, bridgeId);
    long recordsRead = jdesc.read(io.output());

    io.output().println("Read " + recordsRead + " record(s).");

    return 0;
  }
}
