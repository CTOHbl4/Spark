import sys

if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient
import time
import argparse
import signal
import logging

logger = logging.getLogger("python-consumer")
level = logging.WARN


def signal_handler(signal, frame, admin):
    admin.delete_topics(("output",), 3000)
    logger.log(level, "consumer: output topic is deleted")
    sys.exit(0)


TOTAL_LIMIT = 200000000


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num_brokers")
    parser.add_argument("--num_partitions")
    parser.add_argument("--num_workers")

    args = parser.parse_args()

    # logger.log(level, f"num_workers: {args.num_workers}\n\n")

    output_admin = KafkaAdminClient(bootstrap_servers='kafka_output:9093',
                                    client_id="output_admin")

    consumer = KafkaConsumer("output",
                             bootstrap_servers='kafka_output:9093',
                             group_id='test-consumer-group')
    signal.signal(signal.SIGINT,
                  lambda signal, frame: signal_handler(signal,
                                                       frame,
                                                       output_admin))
    signal.signal(signal.SIGTERM,
                  lambda signal, frame: signal_handler(signal,
                                                       frame,
                                                       output_admin))

    total = 0
    timer = time.time()
    logger.log(level, "consumer has started measuring the time\n")
    last_processed_day = "2024-01-01 00:00:00"

    # Analyze messages from the topic.
    for msg in consumer:
        spendings = float(msg.value.decode("utf-8"))

        total += spendings

        logger.log(level, f"current total/limit: {total}/{TOTAL_LIMIT}\n")
        if last_processed_day < (tmp := msg.key.decode("utf-8")):
            last_processed_day = tmp
            logger.log(level, f"current datetime: {last_processed_day}\n\n")

        if total >= TOTAL_LIMIT:
            break

    timer = time.time() - timer
    logger.log(level, f"final duration in seconds: {timer}\n\n")

    # Write a line to a volume with all the previous results.
    f = open("/vol/timers", "a")
    f.write(",".join([str(args.num_brokers),
            str(args.num_partitions),
            str(args.num_workers),
            str(timer) + str(last_processed_day)]) + "\n")
    f.close()

    signal.raise_signal(signal.SIGINT)


if __name__ == "__main__":
    main()
