#!/bin/bash

SERVER_NAME=$1
APP_PORT=$2
EXECUTION_PATH=$3
START_COMMAND=$4
MODE=$5
LOG_FILE=$6

# Source the logging script
if ! source "$(pwd)"/src/test/resources/scripts/local/util.sh; then
    echo "Failed to source util.sh"
    exit 1
fi

log "INFO" "Server start script started..."

setup_util "$SERVER_NAME" "$EXECUTION_PATH" "$MODE" "$APP_PORT" "$LOG_FILE"

log "DEBUG" "Server Name: $SERVER_NAME | EXECUTION_PATH: "$EXECUTION_PATH" | Start Command: $START_COMMAND | Mode: $MODE | Check Port: $APP_PORT"

FULL_COMMAND="$EXECUTION_PATH$START_COMMAND -daemon $EXECUTION_PATH/etc/jms-bridge/jms-bridge.conf"

log "DEBUG" "Start command: $FULL_COMMAND"
log "DEBUG" "Starting $SERVER_NAME in $MODE mode..."

$FULL_COMMAND > /dev/null 2>&1 &

if [ $? -eq 0 ]; then
    log "INFO" "Successfully executed the start command."
else
    log "INFO" "Failed to execute the start command."
    exit 1
fi

# Wait for the JMS server to start by checking the port
check_jms_server "$SERVER_NAME" "$SERVER_HOST" "$MODE" "$APP_PORT" "$LOG_FILE"

if [  $? -eq 0 ]; then
    log "INFO" "JMS server successfully started."
    exit 0
else
    log "INFO" "JMS server did not start within the expected time."
fi
exit 1
