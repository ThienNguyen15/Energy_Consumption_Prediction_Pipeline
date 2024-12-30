import paho.mqtt.client as mqtt
import time
import random
import json
import threading
import argparse
import pandas as pd
from confluent_kafka import Producer
from datetime import datetime, timedelta

KAFKA_TOPIC = 'square_footage'
KAFKA_BOOTSTRAP_SERVERS = ['localhost:29092', 'localhost:39092', 'localhost:49092']
producer = Producer({'bootstrap.servers': ','.join(KAFKA_BOOTSTRAP_SERVERS)})

partition = 4

parser = argparse.ArgumentParser()
parser.add_argument("--thread", type=int, default=4, help='setting thread')
parser.add_argument("--delay", type=int, default=5, help='setting delay time')
csv_file = r"C:\GIT\Distributed Systems\Energy_consumption.csv"

args = parser.parse_args()
_thread, _delay = args.thread, args.delay

df = pd.read_csv(csv_file)

def generate_square_footage(num):
    partition_id = num % partition
    for index in range(len(df)):
        try:
            base_footage = df['SquareFootage'].iloc[index]

            if num == 0:
                footage_value = base_footage
            else:
                footage_value = base_footage + random.uniform(-0.001, 0.001)

            base_timestamp = datetime.strptime(df['Timestamp'].iloc[index], '%m/%d/%Y %H:%M')
            timestamp = base_timestamp + timedelta(minutes=num)

            msg = {
                "deviceId": str(num),
                "timestamps": timestamp.strftime('%m/%d/%Y %H:%M'),
                "value": footage_value,
                "unit": "sqft",
            }

            producer.produce(KAFKA_TOPIC, partition=partition_id, value=json.dumps(msg))
            producer.poll(1)

            print("Published message:", msg)
            time.sleep(_delay)
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"Error: {e}")
            break

if __name__ == '__main__':
    thread_lst = []
    for i in range(_thread):
        t = threading.Thread(target=generate_square_footage, args=(i,))
        t.start()
        thread_lst.append(t)

    for t in thread_lst:
        t.join()

    print("Done!")