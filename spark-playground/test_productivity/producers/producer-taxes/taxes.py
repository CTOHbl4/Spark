from producer import Producer

topic = "taxes"


if __name__ == "__main__":
    producer = Producer(topic)
    producer.produce()
