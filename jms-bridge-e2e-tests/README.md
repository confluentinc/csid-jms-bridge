Tests executed against live and backup jms bridge instances deployed in the live/ and backup/ sub directories.

Relies on docker-compose in jms-bridge-ha to run kafka.

to execute:

- uncomment ```<!--Enable to run e2e tests
        <module>jms-bridge-e2e-tests</module>
        -->``` module in parent POM.
- run ```mvn clean package -DskipTests``` in the root directory to produce JMS Bridge libraries in `jms-bridge-server/target`
- copy libraries from `jms-bridge-server/target/jms-bridge-server-3.3.6-SNAPSHOT-development/share/java/jms-bridge` to `live/share/java/jms-bridge` and `backup/share/java/jms-bridge` directories
- execute tests in `JMSQueueTest` java. 
- For each test logs from jms bridge servers are written to `live/logs` and `backup/logs` directories and collected (if downloadLogs flag is set) to `/src/test/logs/`.
- Kafka container and KStream state is reset between each test for isolation.