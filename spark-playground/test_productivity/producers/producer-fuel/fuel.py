from producer import Producer

topic = "fuel"


if __name__ == "__main__":
    producer = Producer(topic)
    producer.produce()
