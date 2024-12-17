import random
import string
from kafka import KafkaProducer
import json
import time
import pandas as pd

pd.set_option('display.max_columns', None)  
pd.set_option('display.width', None)  

df = pd.read_json('./data/tweets.json', lines=True)  

print(df)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',  # Kafka server
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

topic = 'testing'

def convert_to_serializable(row):
    return {k: v.isoformat() if isinstance(v, pd.Timestamp) else v for k, v in row.items()}

def stream_data_to_kafka(batch_size=100, interval=10):
    total_rows = len(df)
    index = 0

    while index < total_rows:
        batch = df.iloc[index:index + batch_size]
        
        for _, row in batch.iterrows():
            serializable_row = convert_to_serializable(row)
            producer.send(topic, serializable_row)  
            print(f"Sent to Kafka: {serializable_row}")
        
        index += batch_size
        time.sleep(interval)  
stream_data_to_kafka()
