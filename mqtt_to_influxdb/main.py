
from __future__ import unicode_literals
import re
import time
import json
import redis
import paho.mqtt.client as mqtt
from tsdb.worker import Worker
from frappe_api.device_db import DeviceDB


match_data = re.compile(r'^([^/]+)/data/([^/]+)/([^/]+)/(.+)$')
match_devices = re.compile(r'^([^/]+)/devices$')
match_status = re.compile(r'^([^/]+)/status$')

redis_srv = "redis://localhost:6379/8"
redis_db = redis.Redis.from_url(redis_srv)

workers = {}
device_map = {}


def create_worker(db):
	worker = workers.get(db)
	if not worker:
		worker = Worker(db)
		worker.start()
		workers[db] = worker
	return worker

ddb = DeviceDB(redis_srv, device_map, create_worker)
ddb.start()


def get_worker(iot_device):
	worker = device_map.get(iot_device)
	if not worker:
		db = redis_db.get(iot_device).decode('utf-8') or "example"
		worker = create_worker(db)
		device_map[iot_device] = worker
	return worker


def get_input_type(val):
	if isinstance(val, int):
		return "int", val
	elif isinstance(val, float):
		return "float", val
	else:
		return "string", str(val)


inputs_map = {}


def get_input_vt(iot_device, device, input, val):
	t, val = get_input_type(val)
	if t == "string":
		return "string", val

	key = iot_device + "/" + device + "/" + input
	vt = inputs_map.get(key)

	if vt:
		return vt, int(val)

	return None, float(val)


def make_input_map(iot_device, cfg):
	for dev in cfg:
		inputs = cfg[dev].get("inputs")
		if not inputs:
			return
		for it in inputs:
			vt = it.get("vt")
			if vt:
				key = iot_device + "/" + dev + "/" + it.get("name")
				inputs_map[key] = vt


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
			worker = get_worker(g[0])
			prop = g[3]
			value=payload[1]
			if prop == "value":
				t, val = get_input_vt(g[0], g[1], g[2], value)
				if t:
					prop = t + "_" + prop
				value = val
			else:
				value = str(value)
			worker.append_data(name=g[2], property=prop, device=g[1], iot=g[0], timestamp=payload[0], value=value, quality=payload[2])
		return

	g = match_devices.match(msg.topic)
	if g:
		g = g.groups()
		print(g[0], msg.payload)
		worker = get_worker(g[0])
		worker.append_data(name="iot_device", property="cfg", device=g[0], iot=g[0], timestamp=time.time(),
							value=msg.payload.decode('utf-8'), quality=0)
		make_input_map(g[0], json.loads(msg.payload.decode('utf-8')))
		return

	g = match_status.match(msg.topic)
	if g:
		g = g.groups()
		worker = get_worker(g[0])
		#redis_sts.set(g[0], msg.payload.decode('utf-8'))
		status = msg.payload.decode('utf-8')
		if status == "ONLINE" or status == "OFFLINE":
			val = status == "ONLINE"
			worker.append_data(name="device_status", property="online", device=g[0], iot=g[0], timestamp=time.time(),
								value=val, quality=0)
		return


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

