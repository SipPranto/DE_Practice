from confluent_kafka import Consumer
from pymongo import MongoClient
from pymongo.server_api import ServerApi  # This is the correct import
import json
from datetime import datetime
import logging


kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'SWJ4E7VFKNXBEOHF',
    'sasl.password': 'gpbsFTK2INYCNIYNNIWYiVldnAeTdpiXZFq4pzsXUK6hjCFaApWat7j5pFqvhY25',
    'group.id': 'sentiment_analysis_group',
    'auto.offset.reset': 'earliest'
}

KAFKA_TOPIC  = "customer_click_data"

# MongoDB Configuration
uri = "mongodb+srv://sudhanshu:sudh1234@sudhanshu.aprk5.mongodb.net/?retryWrites=true&w=majority&appName=sudhanshu"

def connect_mongo():
    client = MongoClient(uri,server_api=ServerApi('1'))
    return client

def process_message():
    consumer = Consumer(kafka_config)
    consumer.subscribe([KAFKA_TOPIC])
    
    mongo_client = connect_mongo()
    db = mongo_client["cutomer_click_behaviour"]
    event_collection = db["click_data"]
    
    try :
        while True :
            msg = consumer.poll(1.0)   
            if msg is None :
                continue
            if msg.error():
                continue
            try :
                value = json.loads(msg.value().decode('utf-8'))
                value["kafka_metadata"] = {
                    'topic':msg.topic(),
                    "partition" : msg.partition(),
                    "offset" : msg.offset(),
                    "timestamp":datetime.now().isoformat()
                }
                
                result = event_collection.insert_one(value)
                print(result)
            except Exception as e :
                print(e)
                
    except Exception as e :
        print(e)
            
if __name__ == "__main__":
    process_message()