These jars are generated from the main ActiveMQ Artemis github repo,
https://github.com/apache/activemq-artemis.

They are not avaliable via any maven repository I could find so they are instead built and placed
here for now.

The modules from the github project that produce these jars are at:
 - activemq-artemis/tests/artemis-test-support
 - activemq-artemis/tests/unit-tests
 - activemq-artemis/tests/integration-tests

To Build
--------

Clone the main repo:

  git clone git@github.com:apache/activemq-artemis.git

Check out the tag/version (2.13.0 used here), you'll be in a detached state
which is fine for building:

  git checkout 2.13.0

Build it with maven:

  mvn clean package -DskipTests

Copy out the jar files from the ./target folder in each module we need the test jar for.

