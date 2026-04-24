from producer import Producer

topic = "shopping"


if __name__ == "__main__":
    producer = Producer(topic)
    producer.produce()
