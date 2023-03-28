# jms-bridge-acls.py

This script creates the ACLs needed to protect the topics managed by JMS Bridge. 

It either creates the ACLs or outputs the required `kafka-acls` commands (`--dry-run`option). 

It requires at least 2 parameters: 

1. The Kafka bootstrap server.
2. The Principal to setup ACLs for.
3. The JMS Bridge ID.

Topic names are taken from a file (default = `topics.txt`), with each line containing a topic name. 
Topic names can have a placeholder for substituting the JMS Bridge ID and/or a `*` to act as a prefix to protect multiple topics at once. See the supplied `topics.txt` for examples.

Command-line:

```
usage: jms-bridge-acls.py [-h] [--command-config COMMAND_CONFIG]
                          [--topics-file TOPICS_FILE]
                          [--dry-run | --no-dry-run]
                          bootstrap principal bridge_id
```

Example use:

`jms-bridge-acls.py kafka01:9092 jms_bridge_user 1 --command-config command.properties --dry-run`

