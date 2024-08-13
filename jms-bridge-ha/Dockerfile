FROM eclipse-temurin:17-jdk-jammy

ARG JAVA_FILE

# Install curl
RUN apt-get update && apt-get install -y curl

# Install JBang and start a new shell. required for jbang to be found as entrypoint
RUN curl -Ls https://sh.jbang.dev | bash -s - app setup

ENV PATH="/root/.jbang/bin:${PATH}"

# Add your JBang script to the container
COPY $JAVA_FILE /app/script.java

WORKDIR /app

# Run the JBang script
ENTRYPOINT ["jbang", "script.java"]