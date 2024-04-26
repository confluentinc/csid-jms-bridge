# Performance Testing

This module is dedicated to the performance testing of the JMS Bridge.
In order to do the performance testing the csid-jms-bridge repository must be cloned to your workstation.

`git@github.com:confluentinc/csid-jms-bridge.git`

## Workstation
The following software is required to be installed on your workstation to execute the tests:

1. gcloud (GCP)
1. terraform
1. ansible
1. jq

For MacOs users these can be installed via `brew`.

* google-cloud-sdk:  `brew install google-cloud-sdk`
    * Use `gcloud auth application-default login` to initialize your credentials for confluent GCP

* terraform: `brew install terraform`   
    * Minimal version of v0.15.1 
    
* ansible: `brew install ansible`   
    * Minimal version of 2.10.8, *maybe 2.11 due to changes in GCP inventory module*
    
* jq: `brew install jq`

## Execution

The script `execute-test.sh` found in the `${REPO_ROOT}/jms-bridge-perf/bin` is designed to make starting the tests easy.

It does several things
## Resources
The following resources will also need to be made available

1. A Confluent Cloud cluster
    
* Confluent Cloud cluster: 
    * Create or use an existing one deployed in the same target GCP zone as the other nodes

2. An archive release of the JMS-Bridge

* This can be done by building locally
    * From the root of the repo: `mvn clean package`
    * Archive will be at `jms-bridge-server/target/jms-bridge-server-<VERSION>-package.zip` 
    
## Initializing The Environment

From the `tf/` directory run
 * `terraform init`
 * `terraform plan`
 * `terraform apply`

Once applied several instances will be running in GCP.
 * bridge-perf-controller (runs prometheus and grafana)
 * bridge-perf-bridge (the JMS Bridge)
 * bridge-perf-jmeter-[0-3] (4 jmeter nodes)

Create an ansible variable file, as shown below, it contains information for connecting to Kafka.
```json
{
    "jms_bridge_cfg": {
        "id": "load-test-1"
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
    },

    "perf_kafka_consumer_cfg": {
        "bootstrap.servers": "<bootstrap-urls>",
        "sasl.jaas.config": "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"<username>\" password=\"<password/token>\";",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "client.dns.lookup": "use_all_dns_ips"
    }
}
```

From the `ansible/` directory provision the instances via the ansible playbook `bridge-playbook`

>> Note: for the user (-u) specify your GCP account email replacing all special characters with underscores.
```shell
ansible-playbook \
    -u <gcp_email_with_underscores_domain_com>
    --private-key ~/.ssh/google_compute_engine
    -i inventory.gcp.yml \
    --extra-vars "@jms-bridge-vars.json" \
    bridge-playbook.yml
```

### PSY CC PUSH TEST
```shell
echo "Testing 1 2 3"
```
