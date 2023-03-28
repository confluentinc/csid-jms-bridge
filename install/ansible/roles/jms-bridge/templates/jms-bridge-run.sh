#!/bin/bash

_pr_port=8888
_pr_conf=etc/jms-bridge/jmx-prometheus-config.yml
_pr_jar=lib/jms-bridge/jmx-prometheus-javaagent/jmx_prometheus_javaagent-0.15.0.jar

echo "Prometheus jmx agent enabled, port: ${_pr_port}, conf: ${_pr_conf}"

export LOG_DIR=${LOGS_DIRECTORY}
export BRIDGE_STREAMS_STATE_DIR=${STATE_DIRECTORY}
export JMS_BRIDGE_DATA_DIR=${STATE_DIRECTORY}

JMS_BRIDGE_OPTS="-XX:+HeapDumpOnOutOfMemoryError -XX:HeapDumpPath=${STATE_DIRECTORY}"
JMS_BRIDGE_OPTS="${JMS_BRIDGE_OPTS} -javaagent:${_pr_jar}=${_pr_port}:${_pr_conf}"
export JMS_BRIDGE_OPTS
export JMS_BRIDGE_DATA_DIR=${STATE_DIRECTORY}/jms-data
export JMX_PORT=9010
export JMS_BRIDGE_HEAP_OPTS="-Xmx16g"
bin/jms-bridge-server-start etc/jms-bridge/jms-bridge.conf
