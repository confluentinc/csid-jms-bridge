rm Consumer.jar
rm Producer.jar
rm -rf lib

./jbang export portable Consumer.java
docker build -f DockerfileConsumer -t jms-bridge-ha-consumer:latest .
rm Consumer.jar
rm -rf lib

./jbang export portable Producer.java
docker build -f DockerfileProducer -t jms-bridge-ha-producer:latest .
rm Producer.jar
rm -rf lib

