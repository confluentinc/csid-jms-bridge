#!/usr/bin/python3
import argparse
import subprocess
from typing import Dict


BRIDGE_ID_PLACEHOLDER = "{{BRIDGE_ID_PLACEHOLDER}}"


def run_command(command):
    print(command)
    command_as_list = command.split(" ")
    result = subprocess.run(command_as_list, stdout=subprocess.PIPE)
    return result.stdout.decode('utf-8')


def handle_arguments():
    parser = argparse.ArgumentParser(description="Creates the ACLs required to protect the JMS Bridge topics.")

    parser.add_argument("bootstrap",
                        help="Kafka bootstrap server.")

    parser.add_argument("principal",
                        help="Principal to setup ACLs for.")

    parser.add_argument("bridge_id",
                        help="JMS Bridge ID.")

    parser.add_argument("--command-config",
                        help="Kafka client properties file, to be used by the 'kafka-acls' command.",
                        type=str, default="")

    parser.add_argument("--topics-file",
                        help="File containing the name of the topic to protect (use 'name*' for prefixed).",
                        type=str, default="topics.txt")

    parser.add_argument("--dry-run",
                        help="Shows the 'kafka-acls' commands instead of executing them.",
                        action=argparse.BooleanOptionalAction, default=False)

    return parser.parse_args()


def read_topic_names(filename, bridge_id) -> Dict[str, str]:
    with open(filename) as f:
        data = f.read()
    output = {}
    for l in data.splitlines():
        l = l.strip().replace(BRIDGE_ID_PLACEHOLDER, bridge_id)
        is_prefixed = l.endswith('*')
        if is_prefixed:
            name_part = l[:-1]
            output[name_part] = "prefixed"
        else:
            output[l] = "literal"
    return output


def kafka_acls_cmd(principal, topic_name, operation, permission, bootstrap,
                   command_config=None, pattern_type="literal"):
    permission_str = "--allow-principal" if permission == 'allow' else "--deny-principal"

    action_str = "--add"

    if operation != 'read_write':
        operation_str = "--operation {}".format(operation.lower())
    else:
        operation_str = "--operation read  --operation write"

    command_config_str = "--command-config {}".format(command_config) if command_config is not None else ""

    cmd_str = "kafka-acls --bootstrap-server {bootstrap} {action} " \
              "{permission_str} {principal} " \
              "--topic {topic_name} " \
              "--resource-pattern-type {pattern_type} " \
              "{operation} " \
              "{command_config}".format(bootstrap=bootstrap,
                                        action=action_str,
                                        permission_str=permission_str,
                                        principal=principal,
                                        pattern_type=pattern_type,
                                        operation=operation_str,
                                        command_config=command_config_str,
                                        topic_name=topic_name)

    return cmd_str


if __name__ == '__main__':
    args = handle_arguments()

    topics = read_topic_names(args.topics_file, args.bridge_id)

    for topic, pattern_type in topics.items():
        command_str = kafka_acls_cmd(principal=args.principal, topic_name=topic, permission="read_write",
                                     operation="allow", bootstrap=args.bootstrap,
                                     command_config=args.command_config, pattern_type=pattern_type)
        print(command_str) if args.dry_run else run_command(command_str)
