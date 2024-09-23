CURRENT_PATH=$(pwd)
rm -r live/streams-state
rm -r backup/streams-state
rm -r backup/logs
rm -r live/logs
cd ../jms-bridge-ha
docker compose down -v
docker compose up -d kafka
cd $CURRENT_PATH
