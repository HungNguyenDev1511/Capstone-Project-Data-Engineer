import os

#Local directory 
data_directory = "/home/hungnguyen/lake-house-with-minio/data/taxi"

#Kafka topic where you want to save the data
kafka_topic = 'nyc-taxi'  # Replace with your Kafka topic

data = ""
#Iterate over files in the local directory
for filename in os.listdir(data_directory):
    if filename.endswith('.txt'):  # Adjust the file extension to match your data format
        file_path = os.path.join(data_directory, filename)

        with open(file_path, 'r') as file:
            data = file.read()


