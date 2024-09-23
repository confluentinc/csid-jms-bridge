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

MAX_RETRIES=3  # Maximum number of retries
RETRY_INTERVAL=15  # Time (in seconds) between retries

# Function to check if JMS server is up
function check_jms_server {
  if [ "$MODE" = "active" ]; then
    log "INFO" "Checking if JMS server is up on port $APP_PORT..."

    for ((i=1; i<=MAX_RETRIES; i++)); do
        log "INFO" "Iteration number: $i"
        nc -z -w5 localhost $APP_PORT
        if [ $? -eq 0 ]; then
            log "INFO" "JMS server $SERVER_NAME is up and running on port $APP_PORT!"
            return 0
        else
            log "INFO" "JMS server $SERVER_NAME not yet up. Retry $i/$MAX_RETRIES..."
            sleep $RETRY_INTERVAL
        fi
    done
    log "INFO" "JMS server $SERVER_NAME did not start within the expected time."
    return 1

  elif [ "$MODE" = "standby" ]; then
    #log "Checking if backup server is in standby mode..."
    sleep 10
    return 0;
    for ((i=1; i<=MAX_RETRIES; i++)); do
        if lsof -i:$APP_PORT | grep -q 'ESTABLISHED'; then
            log "INFO" "$SERVER_NAME Backup server is up and 'ESTABLISHED' connection found on port $SERVER_NAME!"
            return 0
        else
            log "INFO" "$SERVER_NAME Backup server not yet in standby mode. Retry $i/$MAX_RETRIES..."
            sleep $RETRY_INTERVAL
        fi
    done
    log "INFO" "$SERVER_NAME Backup server did not enter standby mode within the expected time."
    return 1
  else
    log "INFO" "$SERVER_NAME Invalid mode specified. Exiting..."
    exit 1
  fi
}
