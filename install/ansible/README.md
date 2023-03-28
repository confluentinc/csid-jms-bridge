# Ansible installation

## Pre-requisites

- Ansible installed on the workstation performing the installation.
- A VM or server accessible from the workstation performing the installation.

## Execution

1. Create an ansible variable file, as shown below, it contains information for connecting to Kafka.
```json
{
    "jms_bridge_cfg": {
        "id": "test-1"
    },

    "jms_bridge_cfg_journals": {
       "ready.timeout": "5m"
    },

    "jms_bridge_cfg_kafka": {
        "bootstrap.servers": "<bootstrap-urls>",
        "linger.ms": 100,
        "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<username>\" password=\"<password/token>\";",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "client.dns.lookup": "use_all_dns_ips"
    }
}
```

From the `ansible/` directory provision the instances via the ansible playbook `jms-bridge-playbook`

```shell
ansible-playbook \
    -u <admin-user>
    --private-key <admin-user-ssh-key>
    -i inventory.yml \
    --extra-vars "@jms-bridge-vars.json" \
    jms-bridge-playbook.yml
```
