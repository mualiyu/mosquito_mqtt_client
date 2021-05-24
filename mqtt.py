import paho.mqtt.client as mqtt  # import the client1
import time


def on_message(client, userdata, message):
    print(message.topic)
    print(message.payload.decode('utf-8'))


broker_address = "127.0.0.1"

print("creating new instance")
client = mqtt.Client("P1")
client.on_message = on_message
print("connecting to broker")
client.connect(broker_address)
client.loop_start()
print("Subscribing to topic")
client.subscribe("test")
time.sleep(1000000000)
client.loop_stop()
