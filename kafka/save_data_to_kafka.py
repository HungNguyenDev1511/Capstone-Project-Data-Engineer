from save_data_to_kafka import data
from producer import producer


topic = 'nyc-taix'  
message_value = data 

producer.produce(topic, value=message_value)
producer.flush()
