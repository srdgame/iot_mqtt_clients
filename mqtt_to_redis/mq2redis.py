
from __future__ import unicode_literals
import re
import json
import redis
from collections import deque
from configparser import ConfigParser
import paho.mqtt.client as mqtt
from frappe_api.worker import Worker
from redis_client.sub import SubClient

config = ConfigParser()
config.read('../config.ini')


redis_srv = "redis://" + config.get('redis', 'host', fallback='127.0.0.1:6379')
redis_sts = redis.Redis.from_url(redis_srv+"/9")
redis_cfg = redis.Redis.from_url(redis_srv+"/10")
redis_rel = redis.Redis.from_url(redis_srv+"/11")
redis_rtdb = redis.Redis.from_url(redis_srv+"/12")

data_queue = deque()
match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')

device_status = {}


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Main MQTT Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data")
	client.subscribe("+/devices")
	client.subscribe("+/status")


def on_disconnect(client, userdata, rc):
	print("Main MQTT Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_topic.match(msg.topic)
	if not g:
		return
	g = g.groups()
	if len(g) < 2:
		return

	devid = g[0]
	topic = g[1]

	if topic == 'data':
		payload = json.loads(msg.payload.decode('utf-8'))
		g = match_data_path.match(payload[0])
		if g and msg.retain == 0:
			g = g.groups()
			payload.pop(0)
			r = redis_rtdb.hmset(g[0], {
				g[1]: json.dumps(payload)
			})
		return

	if topic == 'devices':
		print(devid, msg.payload)
		redis_rel.ltrim(devid, 0, -1000)
		devs = json.loads(msg.payload.decode('utf-8'))
		for dev in devs:
			redis_cfg.set(dev, json.dumps(devs[dev]))
			redis_rel.lpush(devid, dev)
			if dev == devid:
				worker.create_device(devid, devs[devid])
		return

	if topic == 'status':
		status = msg.payload.decode('utf-8')
		redis_sts.set(devid, status)
		device_status[devid] = status
		worker.update_device_status(devid, status)
		return


# Frappe HTTP API Worker for async call
worker = Worker()
worker.start()
# Redis MQTT message broker
sub = SubClient(redis_srv, config)
sub.start()

# Listen on MQTT forwarding real-time data into redis, and forwarding configuration to frappe.
client = mqtt.Client(client_id="SYS_MQTT_TO_REDIS")
client.username_pw_set("root", "bXF0dF9pb3RfYWRtaW4K")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

mqtt_host = config.get('mqtt', 'host', fallback='127.0.0.1')
mqtt_port = config.get('mqtt', 'port', fallback='1883')
mqtt_keepalive = config.get('mqtt', 'port', fallback=60)
client.connect(mqtt_host, mqtt_port, mqtt_keepalive)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
