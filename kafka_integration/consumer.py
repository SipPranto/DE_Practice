from confluent_kafka import Consumer
from pymongo import MongoClient
import json 
import time 


conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'YGRAJKMOC2EUDELQ',
    'sasl.password': 'VDgvfnZ7T4khoA6FrmzoHz8TESPYh/xjRFJwakHxykXR9gh3R5GAzeviDzJ2kKJ1',
    'group.id':"python-consumer-group",
    'auto.offset.reset':'earliest'
}


# MongoDB Connection
mongo_uri = "mongodb+srv://sudhanshu:1235dfg@sudhanshu.aprk5.mongodb.net/?retryWrites=true&w=majority&appName=sudhanshu"
mongo_client = MongoClient(mongo_uri)
db = mongo_client["kafka_database1"]  # Database name
collection = db["kafka_messages"]  # Collection name



consumer = Consumer(conf)
topic = "euron_sudh"

consumer.subscribe([topic])
print("listening to the msg .........")

try :
    while True :
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"consumer erro : {msg.erro()}")
            continue
        data = json.loads(msg.value().decode('utf-8'))
        
        transformatoin = {
            "index" : data.get("index",0),
            "original_msg" :data.get("message",""),
            "source":"hi from kafka"
        }
        collection.insert_one(transformatoin)
        print(f"stored in mongo: {transformatoin}")
        print(f"i have received this : {data}")
except Exception as e :
    print(e)
finally:
    consumer.close()
    mongo_client.close()
    print("all the data is stored in mongo db")
        