#!/usr/bin/env sh

# Set platform to linux/arm64 if m1 mac is detected. Otherwise set to linux/amd64

if [ "$(uname -m)" = "arm64" ]; then
  PLATFORM=linux/arm64
else
  PLATFORM=linux/amd64
fi

# Prompt for cp version
read -r -e -p "Enter the Confluent Platform Docker Version (default: latest): " CP_VERSION
CP_VERSION=${CP_VERSION:-latest}

# prompt for jms bridge image tag
read -r -e -p "Enter the JMS Bridge Image Tag (default: latest): " JMSBRIDGE_VERSION
JMSBRIDGE_VERSION=${JMSBRIDGE_VERSION:-latest}

# prompt for jms bridge image base
read -r -e -p "Enter the JMS Bridge Image Base (default: placeholder/): " JMSBRIDGE_IMAGE_BASE
JMSBRIDGE_IMAGE_BASE=${JMSBRIDGE_IMAGE_BASE:-placeholder/}


# create .env file from variables set in this file
cat << EOF > .env
PLATFORM=$PLATFORM
CP_VERSION=$CP_VERSION
JMSBRIDGE_VERSION=$JMSBRIDGE_VERSION
JMSBRIDGE_IMAGE_BASE=$JMSBRIDGE_IMAGE_BASE
EOF
