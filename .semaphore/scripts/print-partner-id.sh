#!/bin/bash
#Script to verify and print out Partner-Id entry from manifest inside the jms-bridge-server jar packaged inside a zip archive
#Usage: ./print-partner-id.sh <zip-archive>
#Requires zip and unzip to be installed
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <zip-archive>"
    exit 1
fi
mkdir tmp_partnerid
cp "$1" tmp_partnerid/
CURRENT=$(pwd)
cd tmp_partnerid
ZIPFILE=$(ls -b | grep '.zip')
unzip -o "$ZIPFILE" "jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar"
JARFILE=$(ls -b jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar)
unzip -o "$JARFILE" "META-INF/MANIFEST.MF"
if grep -q "Partner-Id" "META-INF/MANIFEST.MF"; then
    echo "Partner-Id entry found in the manifest:"
    grep "Partner-Id" "META-INF/MANIFEST.MF"
else
    echo "Partner-Id entry not found in the manifest!"
fi
cd "$CURRENT"
rm -rf tmp_partnerid