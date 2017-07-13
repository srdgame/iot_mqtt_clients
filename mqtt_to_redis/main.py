
from __future__ import unicode_literals
import re
import json
import redis
from collections import deque
import paho.mqtt.client as mqtt
from worker.worker import Worker

redis_srv = "redis://localhost:6379"
redis_sts = redis.Redis.from_url(redis_srv+"/9")
redis_cfg = redis.Redis.from_url(redis_srv+"/10")
redis_rel = redis.Redis.from_url(redis_srv+"/11")
redis_rtdb = redis.Redis.from_url(redis_srv+"/12")

data_queue = deque()
match_data = re.compile(r'^([^/]+)/data/([^/]+)/(.+)$')
match_devices = re.compile(r'^([^/]+)/devices$')
match_status = re.compile(r'^([^/]+)/status$')

device_status = {}

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
		#payload = json.loads(msg.payload.decode('utf-8'))
		if msg.retain == 0:
			r = redis_rtdb.hmset(g[1], {
				g[2]: msg.payload.decode('utf-8')
			})
		return

	g = match_devices.match(msg.topic)
	if g:
		g = g.groups()
		print(g[0], msg.payload)
		redis_rel.ltrim(g[0], 0, -1000)
		devs = json.loads(msg.payload.decode('utf-8'))
		for dev in devs:
			redis_cfg.set(dev, json.dumps(devs[dev]))
			redis_rel.lpush(g[0], dev)
			if dev == g[0]:
				worker.create_device(g[0], devs[g[0]])
		return

	g = match_status.match(msg.topic)
	if g:
		g = g.groups()
		status = msg.payload.decode('utf-8')
		redis_sts.set(g[0], status)
		device_status[g[0]] = status
		worker.update_device_status(g[0], status)
		return


worker = Worker()
worker.start()

client = mqtt.Client()
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

client.connect("localhost", 1883, 60)


# Blocking call that processes network traffic, dispatches callbacks and
# handles reconnecting.
# Other loop*() functions are available that give a threaded interface and a
# manual interface.
client.loop_forever()
