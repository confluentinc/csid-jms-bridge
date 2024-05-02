ARG JAVA_FILE_NAME

FROM jbangdev/jbang-action AS build

ARG JAVA_FILE_NAME

RUN mkdir /app
WORKDIR /app
COPY . /app

RUN jbang export portable Producer.java
# make sure the jar is executable
RUN chmod +x /app/Producer.jar

FROM eclipse-temurin:17-jdk-jammy

ARG JAVA_FILE_NAME

RUN mkdir /app/
RUN mkdir /app/lib
COPY --from=build ["/app/Producer.jar", "/app/Producer.jar"]
COPY --from=build /app/lib/* /app/lib/
WORKDIR /app
CMD java -jar "Producer.jar" -t test -d "(tcp://live:61616,tcp://backup:61617)?ha=true&retryInterval=100&retryIntervalMultiplier=1.0&reconnectAttempts=-1&failoverOnServerShutdown=true;" hello