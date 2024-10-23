#!/bin/bash

# Variables from command line arguments
LOCAL_CONFIG_FILE=$1      # Path to the local configuration file
REMOTE_CONFIG_FILE=$2     # Path to the configuration file in JMSBridge deployment
LOG_FILE=$3               # Path to the log file

# Function to log messages
log() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $1" >> "$LOG_FILE"
}

log "Starting the update process for local JMS Bridge deployment..."

# Log the paths for debugging
log "LOCAL_CONFIG_FILE: $LOCAL_CONFIG_FILE"
log "REMOTE_CONFIG_FILE: $REMOTE_CONFIG_FILE"
log "LOG_FILE: $LOG_FILE"

# Check if the local config file exists
if [[ ! -f "$LOCAL_CONFIG_FILE" ]]; then
    log "Local config file does not exist: $LOCAL_CONFIG_FILE"
    exit 1
fi

# Copy the local config file
ERROR=$(cp "$LOCAL_CONFIG_FILE" "$REMOTE_CONFIG_FILE" 2>&1)
EXIT_CODE=$?

if [[ $EXIT_CODE -ne 0 ]]; then
    log "Failed to copy local config file to JMSBridge instance. Error message: $ERROR"
    exit 1
else
    log "Successfully copied the local config file to the JMSBridge instance."
fi

# Optionally compare local file with the remote file
# Check if remote config file exists
if [[ ! -f "$REMOTE_CONFIG_FILE" ]]; then
    log "Remote config file does not exist: $REMOTE_CONFIG_FILE"
    exit 1
fi

# Compare local and remote files
if ! diff -q "$REMOTE_CONFIG_FILE" "$LOCAL_CONFIG_FILE" > /dev/null; then
    log "Differences found."
    # Optionally update the remote file here if needed
else
    log "No differences found. The configuration file is up-to-date."
fi
EOF

log "File modification complete."
