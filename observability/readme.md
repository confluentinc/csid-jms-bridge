# Observability

## Starting point

JMS Bridge provides a JMX-prometheus port to build dashboards.

To enable the prometheus export in docker, set these environment variables:

- `PROMETHEUS_ENABLED=Y`
- `PROMETHEUS_PORT=8888`

And make sure the `jmx-prometheus-javaagent/*` library if copied to `/usr/share/java/`

Mappings from JMX beans to Prometheus counters are in `observability/docker-examples/jms-bridge/image/docker/etc/jmx-prometheus-config.yml`.

TODO: explain how to enable outside of docker

## Examples

In the root of the present folder, the `Health and Performance.json` file provides a Grafana dashboard with Artemis, JVM and Kafka graphs showing the health and performance of the system.

In the `docker-examples` directory you will find:

- A JMS Bridge server along with Kafka a producer and a consumer. In the provided compose file, you have to adjust line 4 to match the desired `jms-bridge` container image. A sample Dockerfile can be found in the `jms-bridge/image` folder. It needs a folder containing compiled JMS Bridge classes and configuration files (inside `bin`, `etc`, `lib`, `share` folders). 

- A full Prometheus + Grafana docker compose suite. To use:
  1. Adjust the `networks` section to match other docker compose containers or the location of the Kafka cluster.
  2. Connect to Grafana on port `3000`. Inside the UI, make sure that the data source is named `prometheus` and adjust the source location if necessary. In the example docker compose JMS bridge, the Prometheus port is `8888`.
  3. Import the `Health and Performance.json` dashboard and enjoy!

