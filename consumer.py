from kafka import KafkaConsumer
import pandas as pd
from json import loads

consumer = KafkaConsumer(bootstrap_servers='masternode:9092')
consumer.subscribe('trungdeptrai')
for message in consumer:
    message = message.value
    print(message)