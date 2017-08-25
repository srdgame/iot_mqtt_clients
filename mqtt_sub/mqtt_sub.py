
from __future__ import unicode_literals
import re
import base64
import json
import binascii
import paho.mqtt.client as mqtt
from configparser import ConfigParser

config = ConfigParser()
config.read('../config.ini')


match_comm = re.compile(r'^([^/]+)/comm$')


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Main MQTT Connected with result code " + str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	print(client.subscribe("+/#"))
	#print(client.subscribe("IDIDIDIDID/app/#"))


def on_disconnect(client, userdata, rc):
	print("Main MQTT Disconnect with result code " + str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	try:
		data = json.loads(msg.payload.decode('utf-8'))
		g = match_comm.match(msg.topic)
		if g:
			raw = base64.b64decode(data[2])
			print(data[0], data[1], binascii.b2a_hex(raw))
			return
		print(msg.topic, data)
	except Exception as ex:
		print(ex)
		print(msg.topic, msg.payload.decode('utf-8')) #, msg.qos, msg.retain)


def on_subscribe(client, userdata, mid, granted_qos):
	print('ON_SUBSCRIBE', mid, granted_qos)


# Listen on MQTT forwarding real-time data into redis, and forwarding configuration to frappe.
client = mqtt.Client(client_id="SYS_MQTT_SUB_CLIENT")
client.username_pw_set("root", "bXF0dF9pb3RfYWRtaW4K")
#client.username_pw_set("changch84@163.com", "pa88word")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message
client.on_subscribe = on_subscribe

mqtt_host = config.get('mqtt', 'host', fallback='127.0.0.1')
mqtt_port = config.getint('mqtt', 'port', fallback=1883)
mqtt_keepalive = config.getint('mqtt', 'port', fallback=60)
client.connect(mqtt_host, mqtt_port, mqtt_keepalive)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
