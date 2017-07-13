
from __future__ import unicode_literals
import re
import time
import json
import sys
import paho.mqtt.client as mqtt
from tsdb.worker import Worker

worker = Worker("example")

match_data = re.compile(r'^([^/]+)/data/([^/]+)/([^/]+)/(.+)$')
match_devices = re.compile(r'^([^/]+)/devices$')
match_status = re.compile(r'^([^/]+)/status$')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data/#")
	client.subscribe("+/devices")
	client.subscribe("+/status")


def on_disconnect(client, userdata, rc):
	print("Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_data.match(msg.topic)
	if g:
		g = g.groups()
		payload = json.loads(msg.payload.decode('utf-8'))
		#print(g[2], payload, msg.topic, msg.retain)
		if msg.retain == 0:
			worker.append_data(name=g[2], property=g[3], device=g[1], iot=g[0], timestamp=payload[0], value=payload[1],
							   quality=payload[2])
		return

	g = match_devices.match(msg.topic)
	if g:
		g = g.groups()
		print(g[0], msg.payload)
		worker.append_data(name="iot_device_cfg", property="value", device=g[0], iot=g[0], timestamp=time.time(),
						   value=msg.payload.decode('utf-8'), quality=0)
		return

	g = match_status.match(msg.topic)
	if g:
		g = g.groups()
		#redis_sts.set(g[0], msg.payload.decode('utf-8'))
		worker.append_data(name="device_status", property="value", device=g[0], iot=g[0], timestamp=time.time(),
						   value=msg.payload.decode('utf-8'), quality=0)
		return


client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.connect("localhost", 1883, 60)


worker.start()
# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()

