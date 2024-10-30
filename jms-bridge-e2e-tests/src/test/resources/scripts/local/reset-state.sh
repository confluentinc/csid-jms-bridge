CURRENT_PATH=$(pwd)
rm -r live/streams-state
rm -r backup/streams-state
cd ../jms-bridge-ha
docker compose down -v
docker compose up -d zookeeper kafka
cd $CURRENT_PATH
