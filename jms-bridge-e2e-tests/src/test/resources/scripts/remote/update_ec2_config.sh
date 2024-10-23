#!/bin/bash

# Variables from command line arguments
PEM_FILE=$1               # Path to your PEM file
USER=$2                   # Username for EC2 instance (e.g., ec2-user)
HOST=$3                   # Public IP or DNS of your EC2 instance
LOCAL_CONFIG_FILE=$4      # Path to the local configuration file
REMOTE_CONFIG_FILE=$5     # Path to the configuration file on EC2
LOG_FILE=$6               # Path to the log file

# Function to log messages
log() {
    echo "$(date +"%Y-%m-%d %H:%M:%S") - $1" >> "$LOG_FILE"
}

log "Starting the update process..."

# Log the paths for debugging
log "PEM_FILE: $PEM_FILE"
log "USER: $USER"
log "HOST: $HOST"
log "LOCAL_CONFIG_FILE: $LOCAL_CONFIG_FILE"
log "REMOTE_CONFIG_FILE: $REMOTE_CONFIG_FILE"
log "LOG_FILE: $LOG_FILE"

# Check if the local config file exists
if [[ ! -f "$LOCAL_CONFIG_FILE" ]]; then
    log "Local config file does not exist: $LOCAL_CONFIG_FILE"
    exit 1
fi

# Copy the local config file to the EC2 instance
#scp -i "$PEM_FILE" "$LOCAL_CONFIG_FILE" "$USER@$HOST:$REMOTE_CONFIG_FILE"
#if [[ $? -ne 0 ]]; then
#    log "Failed to copy local config file to EC2 instance."
#    exit 1
#else
#    log "Successfully copied the local config file to the EC2 instance."
#fi
ERROR=$(scp -i "$PEM_FILE" "$LOCAL_CONFIG_FILE" "$USER@$HOST:$REMOTE_CONFIG_FILE" 2>&1)
EXIT_CODE=$?

if [[ $EXIT_CODE -ne 0 ]]; then
    log "Failed to copy local config file to EC2 instance. Error message: $ERROR"
    exit 1
else
    log "Successfully copied the local config file to the EC2 instance."
fi

# Optionally compare local file with the remote file
ssh -i "$PEM_FILE" "$USER@$HOST" << EOF
# Check if remote config file exists
if [[ ! -f "$REMOTE_CONFIG_FILE" ]]; then
    log "Remote config file does not exist: $REMOTE_CONFIG_FILE"
    exit 1
fi

# Compare local and remote files
if ! diff -q "$REMOTE_CONFIG_FILE" "$LOCAL_CONFIG_FILE" > /dev/null; then
    log "Differences found. Updating EC2 configuration file..."
    # Optionally update the remote file here if needed
else
    log "No differences found. The EC2 configuration file is up-to-date."
fi
EOF

log "File modification complete."
