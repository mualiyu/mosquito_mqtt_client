from __future__ import print_function
from datetime import date, datetime, timedelta
import json
import paho.mqtt.client as mqtt
import time

from mysql import connector as con

broker_address = "127.0.0.1"

db = con.connect(
    host="localhost",
    user="root",
    passwd="",
    database="farm_app"
)
cursor = db.cursor()


def on_message(client, userdata, message):
    created_at = datetime.now()
    msg = message.payload.decode('utf-8').strip()
    hh = json.loads(msg)
    node1 = list(hh['node1'].values())
    node2 = list(hh['node2'].values())

    status = [hh['pump'], hh['operationMode']]

    # for n in nn:
    #     print()
    print(node1, node2, status)
    # print(kk)

    query = "INSERT INTO sensors (sensor_node_id, temperature, humidity, moisture, created_at) VALUES (%s, %s, %s, %s, %s)"

    values = (node1[0], node1[1], node1[2], node1[3], created_at)
    cursor.execute(query, values)

    values2 = (node2[0], node2[1], node2[2], node2[3], created_at)
    cursor.execute(query, values2)

    query2 = "INSERT INTO system_statuses (pump, mode, created_at) VALUES (%s, %s, %s)"
    values3 = (status[0], status[1], created_at)
    cursor.execute(query2, values3)

    db.commit()


print("Runing....")

client = mqtt.Client("M")
client.connect(broker_address)

while True:
    client.loop_start()
    # client.loop_forever()
    client.on_message = on_message
    client.subscribe("smartfarm/dataset")
    # time.sleep(10)
    client.loop_stop()
    # try:
    #     client.loop_start()
    #     client.loop_forever()
    #     client.on_message = on_message
    #     client.subscribe("smartfarm/dataset")
    #     # time.sleep(10)
    #     client.loop_stop()
    # except:
    #     pass
