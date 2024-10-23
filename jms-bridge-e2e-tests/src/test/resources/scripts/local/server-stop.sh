#!/bin/bash

SERVER_NAME=$1
APP_PORT=$2
EXECUTION_PATH=$3
STOP_COMMAND=$4
LOG_FILE=$5
KILL_SERVER=$6

# Source the logging script
if ! source "$(pwd)"/src/test/resources/scripts/local/util.sh; then
    echo "Failed to source util.sh"
    exit 1
fi
log "INFO" "Stop server script started..."

setup_util "$SERVER_NAME" "$EXECUTION_PATH" "" "$APP_PORT" "$LOG_FILE"

log "DEBUG" "Server Name: $SERVER_NAME | EXECUTION_PATH: $EXECUTION_PATH | Stop Command: $STOP_COMMAND | Check Port: $APP_PORT | Kill server: $KILL_SERVER"

if [ "$KILL_SERVER" == "Kill" ]; then
    PID=$(jcmd | grep 'io.confluent.amq.JmsBridgeMain' | grep "$SERVER_NAME" | awk '{print $1}')
    log "DEBUG" "Killing Server Name: $SERVER_NAME | PID: $PID"
    if [ -z "$PID" ]; then
        log "INFO" "No process found to kill"
        exit 1
    fi
    EXECUTE_COMMAND="kill -s KILL $PID"
else
    EXECUTE_COMMAND="$EXECUTION_PATH$STOP_COMMAND"
fi

log "DEBUG" "Server Name: $SERVER_NAME | Execute command: $EXECUTE_COMMAND"

$EXECUTE_COMMAND

if [ $? -eq 0 ]; then
  log "DEBUG" "Successfully executed the stop command."
else
  log "INFO" "Failed to execute the stop command."
  exit 1
fi
sleep 10
nc -z -w5 localhost "$APP_PORT"
if [ $? -ne 0 ]; then
  log "DEBUG" "JMS server is stopped"
  exit 0
else
  log "INFO" "JMS server is not stopped!"
fi
exit 1