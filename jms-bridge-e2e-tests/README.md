Tests executed against live and backup jms bridge instances deployed in the live/ and backup/ sub directories.

Relies on docker-compose in jms-bridge-ha to run kafka.

to execute:

- uncomment ```<!--Enable to run e2e tests
        <module>jms-bridge-e2e-tests</module>
        -->``` module in parent POM.
- run ```mvn clean package -DskipTests``` in the root directory to produce JMS Bridge libraries in `jms-bridge-server/target`
- copy libraries from `jms-bridge-server/target/jms-bridge-server-3.3.7-SNAPSHOT-development/share/java/jms-bridge` to `live/share/java/jms-bridge` and `backup/share/java/jms-bridge` directories
- execute tests in relevant test suites (for example `JMSQueueTest.java`) java. 
- For each test logs from jms bridge servers are written to `live/logs` and `backup/logs` directories and collected (if downloadLogs flag is set) to `/src/test/logs/`.
- Kafka container and KStream state is reset between each test for isolation - the reset logic is setup for execution in `JBTestWatcher.java` - in following fashion:
  - Before each test case execution (`@BeforeEach`) - `reset-instances.sh` is executed - Kafka container is started if not running through docker compose in `jms-bridge-ha` and both live and backup instances of JMS Bridge are terminated (if running / leftover)
  - After each test case execution (`@AfterEach`) - both JMS Bridge instances are stopped (graceful shutdown) and `reset-state.sh` is executed to reset KStreams and Kafka topics state.
  - Note: At the moment actual state reset is implemented only in Local execution mode - remote version of `reset-instances.sh` and `reset-state.sh` are no-op.