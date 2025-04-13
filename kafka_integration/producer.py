from confluent_kafka import Producer
import json 
import time 


conf = {
    'bootstrap.servers': 'pkc-619z3.us-east1.gcp.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'YGRAJKMOC2EUDELQ',
    'sasl.password': 'VDgvfnZ7T4khoA6FrmzoHz8TESPYh/xjRFJwakHxykXR9gh3R5GAzeviDzJ2kKJ1'
}
producer  = Producer(conf)
topic = "euron_sudh"

for i in range(10):
    data = {"index" : i ,"message" : f"hello to the world of kafka by euron {i}" }
    producer.produce(topic,key=str(i),value =json.dumps(data) )
    time.sleep(1)
    
producer.flush()
print("all msg are sent to kafka clusetr")