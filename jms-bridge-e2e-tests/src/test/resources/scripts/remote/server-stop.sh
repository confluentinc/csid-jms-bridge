#!/bin/bash

#AWS EC2 SSH details
SERVER_NAME=$1
APP_PORT=$2
EXECUTION_PATH=$3
STOP_COMMAND=$4
USER=$5
PEM_FILE=$6
LOG_FILE=$7
KILL_SERVER=$8

# Source the logging script
if ! source "$(pwd)"/src/test/resources/scripts/remote/util.sh; then
    echo "Failed to source util.sh"
    exit 1
fi
log "INFO" "Stop server script started..."

setup_util "$SERVER_NAME" "$MODE" "$APP_PORT" "$PEM_FILE" "$USER" "$LOG_FILE"

log "DEBUG" "Server Name: $SERVER_NAME | PEM File: $PEM_FILE | Stop Command: $STOP_COMMAND | Check Port: $APP_PORT | Kill server: $KILL_SERVER"

SSH_COMMAND="ssh -i $PEM_FILE $USER@$SERVER_NAME"

if [ "$KILL_SERVER" == "Kill" ]; then
    EXECUTE_COMMAND="kill -9 \$(jcmd | grep 'io.confluent.amq.JmsBridgeMain' | awk '{print \$1}')"
else
    EXECUTE_COMMAND="$EXECUTION_PATH$STOP_COMMAND"
fi

log "DEBUG" "SSH command: $SSH_COMMAND | Execute command: $EXECUTE_COMMAND"

$SSH_COMMAND << EOF
  $EXECUTE_COMMAND > /dev/null 2>&1 &
EOF

if [ $? -eq 0 ]; then
  log "DEBUG" "Successfully executed the stop command on the EC2 instance."
else
  log "INFO" "Failed to execute the stop command on the EC2 instance."
  exit 1
fi
sleep 10
nc -zv "$SERVER_NAME" "$APP_PORT"
if [ $? -ne 0 ]; then
  log "DEBUG" "JMS server is stopped"
  exit 0
else
  log "INFO" "JMS server is not stopped!"
fi
exit 1