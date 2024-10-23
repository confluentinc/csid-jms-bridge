#!/bin/bash
# Set the log level (DEBUG or INFO)
LOG_LEVEL="DEBUG" # Can be changed to INFO to only log INFO messages

# Function to set up logging with dynamic root path
setup_util() {
    local SERVER_NAME=$1
    local MODE=$2
    local APP_PORT=$3
    local PEM_FILE=$4
    local USER=$5
    local LOG_FILE=$6
    local EXECUTION_PATH=$7
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

MAX_RETRIES=5     # Maximum number of retries
RETRY_INTERVAL=40 # Time (in seconds) between retries
REMOTE_LOG_FILE_PATH="${EXECUTION_PATH}logs/jms-bridge.out"

# Function to check if JMS server is up
function check_jms_server {
    if [ "$MODE" = "active" ]; then
        log "DEBUG" "Checking if JMS server is up on port $APP_PORT..."
        local activeRetryCounter=1
        local previousLogSize=0
        local currentLogSize=0
        for ((activeRetryCounter = 1; activeRetryCounter <= MAX_RETRIES; activeRetryCounter++)); do
            sleep $RETRY_INTERVAL
            log "DEBUG" "Iteration number: $activeRetryCounter"

            # Check if JMS server is up
            nc -zv "$SERVER_NAME" "$APP_PORT"
            if [ $? -eq 0 ]; then
                log "DEBUG" "JMS server is up and running on port $APP_PORT!"
                return 0
            else
                log "DEBUG" "JMS server not yet up. Retry $activeRetryCounter/$MAX_RETRIES..."

                log "DEBUG" "Checking log file $REMOTE_LOG_FILE_PATH for progress..."
                # Check the log size for growth
                currentLogSize=$(ssh -i "$PEM_FILE" "$USER"@"$SERVER_NAME" "stat -c%s '$REMOTE_LOG_FILE_PATH'")
                if [ $currentLogSize -gt $previousLogSize ]; then
                    log "DEBUG" "Log file size is increasing: $currentLogSize bytes."
                    previousLogSize=$currentLogSize # Update log size for next check
                else
                    log "DEBUG" "Log file size has not increased. Assuming server is not making progress."
                    break
                fi
            fi
        done

        log "INFO" "JMS server did not start within the expected time."
        return 1
    elif [ "$MODE" = "standby" ]; then
        local backupRetryCounter=1
        for ((backupRetryCounter = 1; backupRetryCounter <= MAX_RETRIES; backupRetryCounter++)); do
            if ssh -i "$PEM_FILE" "$USER"@"$SERVER_NAME" "lsof -i:$APP_PORT | grep -q 'ESTABLISHED'"; then
                log "DEBUG" "Backup server has ESTABLISHED connections on port $APP_PORT."
                if ssh -i "$PEM_FILE" "$USER"@"$SERVER_NAME" "grep -q 'backup announced' $REMOTE_LOG_FILE_PATH"; then
                    log "DEBUG" "JMS bridge backup server has entered standby mode."
                    return 0
                else
                    log "DEBUG" "Backup server process running but not yet in standby mode."
                fi
            else
                log "DEBUG" "Backup server not yet in standby mode. Retry $backupRetryCounter/$MAX_RETRIES..."
            fi

            sleep $RETRY_INTERVAL
        done
        log "INFO" "Backup server did not enter standby mode within the expected time."
        return 1
    else
        log "INFO" "Invalid mode specified. Exiting..."
        exit 1
    fi
}