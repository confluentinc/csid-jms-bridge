#!/bin/bash
# AWS EC2 SSH details
SERVER_NAME=$1
APP_PORT=$2
SERVER_HOST=$3
COMMAND=$4
MODE="active"
LOG_FILE=$6


# Source the logging script
if ! source "$(pwd)"/src/test/resources/scripts/util.sh; then
    echo "Failed to source util.sh"
    exit 1
fi

log "INFO" "Server check script started..."

setup_util "$SERVER_NAME" "$SERVER_HOST" "$MODE" "$APP_PORT" "$LOG_FILE"

log "DEBUG" "Server Name: $SERVER_NAME | Checking if port is reachable | Mode: $MODE | Check Port: $APP_PORT"

# Wait for the JMS server to start by checking the port
check_jms_server "$SERVER_NAME" "$SERVER_HOST" "$MODE" "$APP_PORT" "$LOG_FILE"

if [  $? -eq 0 ]; then
    log "DEBUG" "JMS server successfully started."
    exit 0
else
    log "INFO" "JMS server not started yet."
fi
exit 1