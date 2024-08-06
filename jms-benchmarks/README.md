# OpenMessaging Benchmarks

The goal of this README is to help you get started with running openmessaging benchmarks against jms-bridge and artemis.

OpenMessaging Benchmark Framework is a suite of tools that make it easy to benchmark distributed messaging systems in
the cloud.

The framework contains two components:

- **Drivers** are responsible for assigning tasks to workers and creating messaging resources (topics, producers
  consumers etc.).
    - Typically, executed using `bin/benchmark` script
- **Workers** are responsible for executing tasks assigned by the driver.
    - Typically, executed using `bin/benchmark-worker --port 8080 --stats-port 8081` script

Example run:

```shell
bin/benchmark \
  --drivers driver-kafka/kafka-exactly-once.yaml \
  --workers worker1:8080,worker2:8080 \
  workloads/1-topic-16-partitions-1kb.yaml
```

## Prerequisites

- Message Broker deployed
- Benchmark workers deployed
- Configure and login to gcloud
    - `gcloud auth configure-docker us-docker.pkg.dev`
    - `gcloud auth login`
    - `gcloud auth application-default login`

## Getting Started (Optional)

Getting started will help walk you through the process of creating and publishing docker images to be used for the
benchmark environment. If you already have these images handy, you may skip this section.

### Cloning and building repo

```shell
git clone git@github.com:openmessaging/benchmark.git && cd benchmark
```

### Add jms client libraries to package

Within the package module (`package/pom,xml`), add the following dependencies:

```xml

<dependency>
    <groupId>ch.qos.logback</groupId>
    <artifactId>logback-classic</artifactId>
    <version>1.5.3</version>
</dependency>
<dependency>
<groupId>org.apache.activemq</groupId>
<artifactId>artemis-jms-client-all</artifactId>
<version>2.35.0</version>
</dependency>
```

This will allow us to package the jms client libraries with the benchmark framework and have working logs :).

### Build the benchmark framework

```shell
mvn clean verify -DskipTests
```

### Modify the docker file image to use a newer version of java

```dockerfile
# docker/Dockerfile
FROM eclipse-temurin:17
# rest of the file
```

### Build the docker image

```shell
export BENCHMARK_TARBALL=package/target/openmessaging-benchmark-0.0.1-SNAPSHOT-bin.tar.gz
docker build --platform=linux/amd64,linux/arm64 --build-arg BENCHMARK_TARBALL -t us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest -f docker/Dockerfile .
```

### Push the docker image to artifact registry

```shell
docker push us-docker.pkg.dev/csid-281116/csid-docker-repo-us/benchmarks:latest
```

### Build the jms bridge image

```shell
# from the root of csid-jms-bridge
mvn clean package -DskipTests -Dspotbugs.skip -Dcheckstyle.skip
# build the jms docker image
docker build --platform=linux/amd64 -t us-docker.pkg.dev/csid-281116/csid-docker-repo-us/jms-bridge-docker:3.3.4-SNAPSHOT --build-arg DOCKER_UPSTREAM_REGISTRY='' --build-arg DOCKER_UPSTREAM_TAG=latest --build-arg PROJECT_VERSION=3.3.4-SNAPSHOT --build-arg ARTIFACT_ID=jms-bridge-docker -f Dockerfile .
# push it to the artifact registry
 docker push us-docker.pkg.dev/csid-281116/csid-docker-repo-us/jms-bridge-docker:3.3.4-SNAPSHOT
```

## Deploy Benchmark

See [sample.terraform.tfvars](benchmark-demo/environments/gcp/csid/sample.terraform.tfvars) for an example
configuration.

### Deploy terraform

```shell
cd benchmark-demo
# from the benchmark-demo directory
terraform init
terraform apply --var-file=environments/gcp/csid/terraform.tfvars
```