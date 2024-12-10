#!/bin/bash
#Script to update manifest inside the jms-bridge-server jar packaged inside a zip archive
#Usage: ./update-partner-id.sh <zip-archive> <partner-id>
#Note: If Partner-Id entry is already present in the manifest - it will be overwritten
#Requires jar, zip and unzip to be installed
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 <zip-archive> <partner-id>"
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
    echo "Partner-Id entry already present in the manifest! Will be replaced with $2"
    sed 's/Partner-Id: .*/Partner-Id: '"$2"'/g' "META-INF/MANIFEST.MF" >> "META-INF/MANIFEST.MF.tmp"
    mv "META-INF/MANIFEST.MF.tmp" "META-INF/MANIFEST.MF"
    zip "$JARFILE" "META-INF/MANIFEST.MF"
else
    echo "Adding new Partner-Id: $2 entry to the Manifest"
    printf "Partner-Id: %s\n" "$2" >> "partnerid.tmp"
    jar uvfm jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar partnerid.tmp
fi
zip "$ZIPFILE" "jms-bridge-server*/share/java/jms-bridge/jms-bridge-server*.jar"
cd "$CURRENT"
cp "tmp_partnerid/$ZIPFILE" "$1"
rm -rf tmp_partnerid