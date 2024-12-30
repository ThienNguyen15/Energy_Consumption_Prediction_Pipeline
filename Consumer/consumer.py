import paho.mqtt.client as mqtt

def on_subscribe(client, userdata, mid, granted_qos):
    print("Subscribed: " + str(granted_qos))

def on_message(client, userdata, msg):
    print(msg.topic + " " + str(msg.qos) + " " + str(msg.payload))

client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
client.on_subscribe = on_subscribe
client.on_message = on_message

client.connect('localhost', 1883)

client.subscribe('ac', qos=1)
client.subscribe('alarm', qos=1)
client.subscribe('light', qos=1)
# qos = 1, qos = 2

client.loop_forever()
