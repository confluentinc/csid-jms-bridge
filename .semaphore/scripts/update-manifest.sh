#!/bin/bash
#Script to update manifest inside the jms-bridge-server jar packaged inside a zip archive
#Usage: ./update-manifest.sh <zip-archive> <partner-id>
#Note: If Partner-Id entry is already present in the manifest - it will be overwritten
#Requires jar, zip and unzip to be installed
unzip -o "$1" "jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar"
printf "Partner-Id: %s\n" "$2" >> "partnerid.tmp"
jar uvfm jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar partnerid.tmp
rm partnerid.tmp
zip "$1" "jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar"
rm -r "$(ls -b -d jms*| grep -v '.zip')"
