echo "Resetting instances"
CURRENT_PATH=$(pwd)
cd ../jms-bridge-ha || { echo "Failed to change directory"; exit 1; }
echo "Docker compose up in the path :$(pwd)"
docker compose up -d zookeeper kafka 2>&1 | tee docker_output.log
echo "Docker compose up done"
cd $CURRENT_PATH
echo "Curent path after docker compose up : $CURRENT_PATH"
src/test/resources/scripts/local/server-stop.sh live 61616 live /bin/jms-bridge-server-stop /dev/null Kill
src/test/resources/scripts/local/server-stop.sh backup 61617 backup /bin/jms-bridge-server-stop /dev/null Kill
rm -r backup/logs
rm -r live/logs
echo "Instances reset"
sleep 10
exit 0