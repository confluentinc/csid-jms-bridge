CURRENT_PATH=$(pwd)
cd ../jms-bridge-ha
docker compose up -d kafka
cd $CURRENT_PATH
src/test/resources/scripts/local/server-stop.sh live 61616 live /bin/jms-bridge-server-stop /dev/null Kill
src/test/resources/scripts/local/server-stop.sh backup 61617 backup /bin/jms-bridge-server-stop /dev/null Kill
rm -r backup/logs
rm -r live/logs
exit 0