from confluent_kafka import Consumer, KafkaException, KafkaError
import json
import time
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), 'C:\\GIT\\Distributed Systems\\DISTRIBUTED_BTL\\db_prediction.py', 'DISTRIBUTED_BTL')))
from DISTRIBUTED_BTL.db_prediction import *

KAFKA_TOPICS = ['temperature', 'humidity', 'square_footage', 'renewable_energy', 'energy_consumption']
KAFKA_BOOTSTRAP_SERVERS = 'localhost:29092,localhost:39092,localhost:49092'
DB_URL = 'postgresql://neondb_owner:SkYV5t7DpHoi@ep-long-grass-a57cixig.us-east-2.aws.neon.tech/neondb?sslmode=require'

def create_consumer():
    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'multi_topic_consumer',
        'auto.offset.reset': 'earliest',
    }
    return Consumer(consumer_config)

def consume_messages():
    consumer = create_consumer()
    consumer.subscribe(KAFKA_TOPICS)

    data_buffer = {}
    complete_records = []

    try:
        print("Listening to Kafka topics:", KAFKA_TOPICS)
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                if complete_records:
                    record = complete_records.pop(0)
                    print(json.dumps(record, indent=2))
                    insert_energy_data(DB_URL, record['Temperature'], record['Humidity'], record['SquareFootage'], record['RenewableEnergy'],
                                       record['EnergyConsumption'], record['Timestamp'])
                    # print("Check", get_latest_data_from_neon(DB_URL, window_size=7))
                    time.sleep(1)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())

            message_value = msg.value().decode('utf-8')
            try:
                row_data = json.loads(message_value)
                topic = msg.topic()

                timestamp = row_data['timestamps']
                deviceId = row_data['deviceId']

                if timestamp not in data_buffer:
                    data_buffer[timestamp] = {}

                if deviceId not in data_buffer[timestamp]:
                    data_buffer[timestamp][deviceId] = {
                        'DeviceId': None,
                        'Temperature': None,
                        'Humidity': None,
                        'SquareFootage': None,
                        'RenewableEnergy': None,
                        'EnergyConsumption': None,
                        'complete': False
                    }

                if topic == 'temperature':
                    data_buffer[timestamp][deviceId]['DeviceId'] = int(deviceId)
                    data_buffer[timestamp][deviceId]['Temperature'] = float(row_data['value'])

                elif topic == 'humidity':
                    data_buffer[timestamp][deviceId]['Humidity'] = float(row_data['value'])

                elif topic == 'square_footage':
                    data_buffer[timestamp][deviceId]['SquareFootage'] = float(row_data['value'])

                elif topic == 'renewable_energy':
                    data_buffer[timestamp][deviceId]['RenewableEnergy'] = float(row_data['value'])

                elif topic == 'energy_consumption':
                    data_buffer[timestamp][deviceId]['EnergyConsumption'] = float(row_data['value'])

                entry = data_buffer[timestamp][deviceId]
                if not entry['complete']:
                    if all(entry[key] is not None for key in
                           ['DeviceId', 'Temperature', 'Humidity', 'SquareFootage', 'RenewableEnergy', 'EnergyConsumption']):
                        result = {
                            "Timestamp": timestamp,
                            "DeviceId": entry['DeviceId'],
                            "Temperature": entry['Temperature'],
                            "Humidity": entry['Humidity'],
                            "SquareFootage": entry['SquareFootage'],
                            "RenewableEnergy": entry['RenewableEnergy'],
                            "EnergyConsumption": entry['EnergyConsumption']
                        }

                        complete_records.append(result)
                        entry['complete'] = True

            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON: {e}")

    except KeyboardInterrupt:
        print("\nExiting consumer...")
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages()
