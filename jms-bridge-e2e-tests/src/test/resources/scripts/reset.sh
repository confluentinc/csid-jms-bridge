CURRENT_PATH=$(pwd)
src/test/resources/scripts/server-stop.sh live 61616 live /bin/jms-bridge-server-stop /dev/null Kill
src/test/resources/scripts/server-stop.sh backup 61617 backup /bin/jms-bridge-server-stop /dev/null Kill
rm -r live/streams-state
rm -r backup/streams-state
rm -r backup/logs
rm -r live/logs
cd ../jms-bridge-ha
docker compose down -v
docker compose up -d kafka
cd $CURRENT_PATH
