#!/bin/bash
# Set the log level (DEBUG or INFO)
LOG_LEVEL="DEBUG"  # Can be changed to INFO to only log INFO messages

# Function to set up logging with dynamic root path
setup_util() {
    local SERVER_NAME="$1"
    local SERVER_HOST="$2"
    local MODE="$3"
    local APP_PORT="$4"
    local LOG_FILE="$5"
}
# Function to handle logging
log() {
    local LEVEL=$1
    local MESSAGE=$2

    # Define log levels and their order
    local LOG_LEVELS=("DEBUG" "INFO")

    # Determine if the current level should be logged
    local LEVEL_INDEX
    local LOG_LEVEL_INDEX

    for i in "${!LOG_LEVELS[@]}"; do
        if [[ "${LOG_LEVELS[i]}" == "$LEVEL" ]]; then
            LEVEL_INDEX=$i
        fi
        if [[ "${LOG_LEVELS[i]}" == "$LOG_LEVEL" ]]; then
            LOG_LEVEL_INDEX=$i
        fi
    done

    if [ $LEVEL_INDEX -ge $LOG_LEVEL_INDEX ]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') - $LEVEL - $MESSAGE" | tee -a "$LOG_FILE"
    fi
}

TIMEOUT_INTERVAL=100  # Time (in seconds) for log monitoring to timeout

# Function to check if JMS server is up
function check_jms_server {
  log "INFO" "Checking if JMS server $SERVER_NAME has started as $MODE"
  if [ "$MODE" = "active" ]; then
    timeout $TIMEOUT_INTERVAL tail -Fn0 "$SERVER_NAME/logs/jms-bridge.out" | grep --line-buffered -q 'Started KQUEUE Acceptor at '
    EXIT_STATUS=$?
    if [ $EXIT_STATUS -eq 124 ]; then
        log "INFO" "JMS server $SERVER_NAME did not start within the expected time."
        return 1
    fi
    return $EXIT_STATUS

  elif [ "$MODE" = "standby" ]; then
    timeout $TIMEOUT_INTERVAL tail -Fn0 "$SERVER_NAME/logs/jms-bridge.out" | grep --line-buffered -q 'started, waiting live to fail before it gets active'
    EXIT_STATUS=$?
    if [ $EXIT_STATUS -eq 124 ]; then
        log "INFO" "JMS server $SERVER_NAME did not start within the expected time."
        return 1
    fi
    return $EXIT_STATUS
  else
    log "INFO" "$SERVER_NAME Invalid mode specified. Exiting..."
    exit 1
  fi
}
