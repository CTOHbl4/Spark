# Base image
FROM ubuntu:latest

# Install dependencies
RUN apt -y update
RUN apt -y install maven openjdk-8-jdk wget

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xzvf hadoop-3.3.6.tar.gz
RUN mv hadoop-3.3.6 /opt/hadoop-3.3.6

# Environment variables
RUN export JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-arm64
RUN export PATH=$JAVA_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-1.21.0-openjdk-arm64
# path to java_home is change based on local pc.

# Source code
COPY task /task
VOLUME /task

# Build package
WORKDIR /task
RUN mvn -X clean package

# Run package
# ENTRYPOINT /opt/hadoop-3.3.6/bin/hadoop jar $JAR_FILE $JAVA_CLASS $TASK_ARGS
