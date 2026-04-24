### Studying Spark and Kafka

# Description

This is a studying project of Spark Structured Streaming. If you are here, then, if you are a student, I recommend you to start with Taxi Heatmap and then Test Productivity.

Big Data is appearing everywhere and demand for specialists in this field only grows, so I consider this project very useful.

# Before you start working on this project, you should:

0) Understand, what Big Data is,

1) have already written at least one working Dockerfile,

2) know python,

3) be friends with bash and terminal.

Otherwise this is going to be a really tough challenge.

# What I have learnt or tried for te first time:

1) I became more fluent with Docker:
    - Docker files: COPY, ENV, WORKDIR, FROM, ENTRYPOINT/CMD with shell/exec forms of commands (and changing these from the original Dockerfile), VOLUME, RUN, variables, correct ways to stop containers (SIGTERM/SIGINT/SIGKILL),
    - Docker compose: networks, volumes, services, different ways of starting containers, container name, ports, environment, command,
2) I gained a better understanding of what a generator is and when it is used,
3) I learnt a code-style in python, studied logging,
4) I studied how to write .sh files: conditions and loops, variables,
5) I successfully experimented with Kafka and Spark Structured Streaming and understood, what these are and how to use these tools.

# A recommended plan of work with comments:

Suppose we have 14 weeks for a full project. Then I would recommend the following plan:

1) 3-4 weeks for a fully usable generator (or just use the Generic Generator).
    - As a backstory for the nature of events and messages I would take a task, where you would aggregate by time (day/year...), as in Spark you are obliged to do this aggregation. So, for example, you collect a daily statistic. If you want more, check the stateful Spark usage.
2) 3 weeks to learn, how Kafka (KRaft/Zookeeper) works (some minor experiments, maybe, with docker-compose-kafka.yml).
3) 3 weeks to learn, how Spark works (attend lectures and some minor experiments).
4) 3-4 weeks for the final project.
