from confluent_kafka import Consumer, Producer, KafkaError
from elasticsearch import Elasticsearch
from textblob import TextBlob
import json
import os
from datetime import datetime


# Kafka Configuration
kafka_config = {
    'bootstrap.servers': 'pkc-921jm.us-east-2.aws.confluent.cloud:9092',
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': 'SWJ4E7VFKNXBEOHF',
    'sasl.password': 'gpbsFTK2INYCNIYNNIWYiVldnAeTdpiXZFq4pzsXUK6hjCFaApWat7j5pFqvhY25',
    'group.id': 'sentiment_analysis_group',
    'auto.offset.reset': 'earliest'
}
es = Elasticsearch(
    "https://37db164996fa4e679a60c7780350b697.us-central1.gcp.cloud.es.io:443",
   api_key = "SExZeExaVUJMamhYdURJa2t6MXY6aW1wNnZ6a2dRU0d4bm1ERGtrVjRBQQ==")

consumer = Consumer(kafka_config)
input_topics = "senti_raw_msg"


def analyze_sentiment(text):
        analysis = TextBlob(text)
        return {
            'text' : text, 
            "polarity" : analysis.sentiment.polarity,
            "subjectivity" : analysis.sentiment.subjectivity,
            'timestamp' : datetime.now().isoformat()
        }   
        
def store_into_elastic( sentiment_data) : 
        es.index(index= "sentiment_analysis" , document = sentiment_data)
        
consumer.subscribe([input_topics])
print("my consumer is runinng ....")

while True :
    msg = consumer.poll(1.0)
    if msg is None :
        continue
    if msg.error():
        print(f"the erro in msg is {msg.error()}")
        continue
    
    message = json.loads(msg.value().decode('utf-8'))
    text = message.get("text","")
    
    
    sentiment_data = analyze_sentiment(text)
    
    store_into_elastic(sentiment_data)    
        
    print(f"all doine till elastic {sentiment_data}")

