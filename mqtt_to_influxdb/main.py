
from __future__ import unicode_literals
import re
import time
import json
import sys
from collections import deque
import paho.mqtt.client as mqtt
import tsdb.client as tsdb


tsdb_client = tsdb.Client()
tsdb_client.connect()
tsdb_client.create_database()

data_queue = deque()
topic_p = re.compile(r'^([^/]+)/data/([^/]+)/(.+)$')
# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data/#")


def on_disconnect(client, userdata, rc):
	print("Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = topic_p.match(msg.topic).groups()
	payload = json.loads(msg.payload.decode('utf-8'))
	if msg.retain == 0:
		data_queue.append({
			"name": g[2],
			"device": g[1],
			"iot": g[0],
			"timestamp": payload[0],
			"value": payload[1],
			"quality": payload[2],
		})


client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.connect("localhost", 1883, 60)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
#client.loop_forever()
client.loop_start()

while True:
	if len(data_queue) == 0:
		time.sleep(0.5)
		continue
	else:
		try:
			points = list(data_queue)
			tsdb_client.write_data(points)
			data_queue.clear()
		except Exception as ex:
			sys.stdout.write('+')
			sys.stdout.flush()
			if len(data_queue) > 100000:
				print("Clear data queue as it is too big!")
				data_queue.clear()
			time.sleep(0.5)

client.loop_end()
