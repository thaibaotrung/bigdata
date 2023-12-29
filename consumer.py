from kafka import KafkaConsumer
import pandas as pd
from json import loads
import csv
consumer = KafkaConsumer('trung', bootstrap_servers='masternode:9092')

with open('output.csv', 'w', newline='') as file:
    writer = csv.writer(file)
    writer.writerow(['Name', 'Price', 'Change', 'Open', 'High', 'Low', 'Volume'])

    for message in consumer:
        row = message.value.decode('utf-8').split(',')
        print(row)
        writer.writerow(row)


print('xong')
