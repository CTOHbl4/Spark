from producer import Producer

topic = "entertainment"


if __name__ == "__main__":
    producer = Producer(topic)
    producer.produce()
