from confluent_kafka import Consumer, Producer, KafkaError
from elasticsearch import Elasticsearch
from textblob import TextBlob
import json
import os
from datetime import datetime
import streamlit as st


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

producer = Producer(kafka_config)
input_topics = "senti_raw_msg"


st.title("this will send data to kafka")
st.write("enter the sentiment so that we can tell you how you are feeling ")

user_input = st.text_area("enter anything that you are feeling right now ")

if st.button("send this msg to kafka"):
    if user_input:
        message = {
            "text" : user_input,
            "timestmap": datetime.now().isoformat()
        }
        producer.produce(input_topics,key = "sentiment" , value = json.dumps(message))
        producer.flush()
        st.success("message sent to kafka")