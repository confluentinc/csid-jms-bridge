#!/bin/bash
# AWS EC2 SSH details
SERVER_NAME=$1
APP_PORT=$2
EXECUTION_PATH=$3
START_COMMAND=$4
CONFIG_FILE=$5
MODE=$6
USER=$7
PEM_FILE=$8
LOG_FILE=$9

# Source the logging script
if ! source "$(pwd)"/src/test/resources/scripts/remote/util.sh; then
    echo "Failed to source util.sh"
    exit 1
fi

log "INFO" "Server start script started..."

setup_util "$SERVER_NAME" "$MODE" "$APP_PORT" "$PEM_FILE" "$USER" "$LOG_FILE" "$EXECUTION_PATH"

log "DEBUG" "Server Name: $SERVER_NAME | PEM File: $PEM_FILE | Start Command: $START_COMMAND | Mode: $MODE | Check Port: $APP_PORT"

FULL_COMMAND="$EXECUTION_PATH$START_COMMAND -daemon $EXECUTION_PATH$CONFIG_FILE"
SSH_COMMAND="ssh -i $PEM_FILE $USER@$SERVER_NAME"
log "DEBUG" "SSH command: $SSH_COMMAND | Full command: $FULL_COMMAND"
log "DEBUG" "Starting $SERVER_NAME in $MODE mode on EC2..."

$SSH_COMMAND << EOF
  $FULL_COMMAND > /dev/null 2>&1 &
EOF

if [ $? -eq 0 ]; then
    log "DEBUG" "Successfully executed the start command on the EC2 instance."
else
    log "INFO" "Failed to execute the start command on the EC2 instance."
    exit 1
fi

# Wait for the JMS server to start by checking the port
check_jms_server "$SERVER_NAME" "$MODE" "$APP_PORT"

if [  $? -eq 0 ]; then
    log "DEBUG" "JMS server successfully started."
    exit 0
else
    log "INFO" "JMS server did not start within the expected time."
fi
exit 1
