from confluent_kafka import Producer

producer_config = {
    # Replace with your Kafka broker(s)
    'bootstrap.servers': 'localhost:9101',  
}

producer = Producer(producer_config)