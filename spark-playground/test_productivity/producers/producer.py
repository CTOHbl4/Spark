import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
import json
import time
import logging
import signal

num_partitions = 1
if len(sys.argv) > 1:
    # sys.argv comes from the __main__
    num_partitions = int(sys.argv[1])
bootstrap_servers = "kafka1:9092"
replication_factor = 1
client_id = "input_admin"


def signal_handler(signal, frame):
    # To call the destructors.
    sys.exit(0)


def filename(topic):
    return f"/app/{topic}.json"


class Producer:
    '''
    Producer is a class, which creates a topic and writes messages to it.

    Attributes
    ----------

    admin_client: KafkaAdminClient
        - An admin to create/delete the topic.

    topic: str
        - A topic's name.

    num_partitions: int
        - Number of partitions for a topic.

    producer: KafkaProducer
        - A producer instance for the topic.

    logger: Logger

    Parameters
    ----------

    topic_name: str
        - A name of the topic.

    num_partitions: int = num_partitions
        - A number of partitions of a kafka topic.

    bootstrap_servers: str = bootstrap_servers
        - Bootstrap servers for kafka.

    replication_factor: int = replication_factor
        - Ammount of times to replicate the date in the topic.

    client_id: str = client_id
        - Any string to name the admin client.

    logger: logging.Logger = None
        - A logger. If None, a new logger is created.
    '''
    def __init__(self,
                 topic_name: str,
                 num_partitions: int = num_partitions,
                 bootstrap_servers: str = bootstrap_servers,
                 replication_factor: int = replication_factor,
                 client_id: str = client_id,
                 logger: logging.Logger = None):
        self.admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id=client_id
        )
        self.topic = topic_name
        topic = NewTopic(name=self.topic,
                         num_partitions=num_partitions,
                         replication_factor=replication_factor)
        self.num_partitions = num_partitions

        try:
            self.admin_client.create_topics(new_topics=(topic,))
        except Exception:
            pass

        signal.signal(signal.SIGINT, signal_handler)
        signal.signal(signal.SIGTERM, signal_handler)

        # See all available partitions for a topic.
        # For some unstudied here reasons, available partitions are not always
        # all of the partitions.
        # cluster = ClusterMetadata(bootstrap_servers=bootstrap_servers)
        # logger.log(level, cluster.available_partitions_for_topic(topic))

        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
        self.logger = logging.getLogger('python-producer') if logger is None \
            else logger

    def produce(self, filename=filename) -> None:
        '''
        Tell the producer to collect new messages into a batch.
        Producer sends batch to kafka brokers once the size of a batch reaches
        a limit.

        The limit may be changed.

        With "partition" parameter we tell the broker, in which exact
        partition to put a message.

        The issue may appear, as available partitions may not be all of them.
        Here we assume that all are available.

        Parameters
        -----------

        filename: function(str) -> str
            A function(topic_name) -> str, from which to take messages.

        Returns
        -------

        None

        '''
        year_num = -1
        for _ in range(50):
            year_num += 1
            with open(filename(self.topic)) as f:
                for idx, line in enumerate(f):
                    jline = json.loads(line)
                    t = jline["timestamp"]
                    t = str(2024 + year_num) + t[4:]
                    jline["timestamp"] = t

                    self.producer.send(self.topic,
                                       value=bytes(json.dumps(jline), "utf-8"),
                                       partition=(idx % self.num_partitions))
                    time.sleep(0.0001)

    def __del__(self) -> None:
        self.admin_client.delete_topics([self.topic], 3000)
        self.logger.log(logging.WARN, f"{self.topic} topic is deleted")
