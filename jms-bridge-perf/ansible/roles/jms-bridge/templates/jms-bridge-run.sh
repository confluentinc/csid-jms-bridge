#!/bin/bash

_prometheus_port=8888
_prometheus_conf=etc/jms-bridge/jmx-prometheus-config.yml
_prometheus_jar=lib/jms-bridge/jmx-prometheus-javaagent/jmx_prometheus_javaagent-0.15.0.jar

echo "Prometheus jmx agent enabled, port: ${_prometheus_port}, conf: ${_prometheus_conf}"

export LOG_DIR=${LOGS_DIRECTORY}
export BRIDGE_STREAMS_STATE_DIR=${STATE_DIRECTORY}
export JMS_BRIDGE_OPTS="-javaagent:${_prometheus_jar}=${_prometheus_port}:${_prometheus_conf}"
export JMS_BRIDGE_HEAP_OPTS="-Xmx66g"
bin/jms-bridge-server-start etc/jms-bridge/jms-bridge.conf
