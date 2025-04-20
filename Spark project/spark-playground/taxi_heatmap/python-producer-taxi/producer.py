import sys
if sys.version_info >= (3, 12, 0):
    import six
    sys.modules['kafka.vendor.six.moves'] = six.moves

from kafka import KafkaProducer
import time
import json


def main():
    # Create producer and connect to a specified bootstrap server.
    producer = KafkaProducer(bootstrap_servers="kafka1:9092")

    cur_time = 0
    while True:
        with open("/app/telem.json") as f:
            for line in f:
                j_line = json.loads(line)
                t = j_line["ts"]
                # Realistic approach: wait before the next timestamp.
                if t > cur_time:
                    time.sleep(t - cur_time)
                    cur_time = t

                producer.send("test-topic",
                              key=bytes(int(t)),
                              value=bytes(line, "utf-8"))


if __name__ == "__main__":
    main()
