import random
import string
from kafka import KafkaProducer
import json
import time
import pandas as pd

# Set pandas options to display all columns
pd.set_option('display.max_columns', None)  # Show all columns
pd.set_option('display.width', None)  # Disable line wrapping for wide DataFrames

# Load the JSON data into a DataFrame (replace with your own file path and method)
df = pd.read_json('./data/tweets.json', lines=True)  # Example if it's newline-separated JSON

# Display the DataFrame
print(df)

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Define your Kafka topic
topic = 'testing'

# Function to convert pandas DataFrame rows to a JSON serializable format
def convert_to_serializable(row):
    # Convert Timestamp objects to ISO format strings
    return {k: v.isoformat() if isinstance(v, pd.Timestamp) else v for k, v in row.items()}

# Stream data to Kafka in batches of 100 rows every 10 seconds
def stream_data_to_kafka(batch_size=100, interval=10):
    total_rows = len(df)
    index = 0

    while index < total_rows:
        # Select the next batch of rows
        batch = df.iloc[index:index + batch_size]
        
        # Send each row in the batch to Kafka
        for _, row in batch.iterrows():
            # Convert the row to a serializable format
            serializable_row = convert_to_serializable(row)
            producer.send(topic, serializable_row)  # Send row data to Kafka topic as a dictionary
            print(f"Sent to Kafka: {serializable_row}")
        
        index += batch_size
        time.sleep(interval)  # Wait for the specified interval before sending the next batch

# Start streaming data
stream_data_to_kafka()
