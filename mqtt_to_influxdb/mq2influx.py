
from __future__ import unicode_literals
import re
import os
import time
import json
import redis
import logging
import zlib
from configparser import ConfigParser
import paho.mqtt.client as mqtt
from tsdb.worker import Worker
from frappe_api.device_db import DeviceDB


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S')

match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/([^/]+)/(.+)$')
match_stat_path = re.compile(r'^([^/]+)/([^/]+)/(.+)$')

config = ConfigParser()
config.read('../config.ini')

redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')
redis_db = redis.Redis.from_url(redis_srv + "/8", decode_responses=True) # device influxdb database

workers = {}
device_map = {}


def create_worker(db):
	worker = workers.get(db)
	if not worker:
		worker = Worker(db, config)
		worker.start()
		workers[db] = worker
	return worker
#
# ddb = DeviceDB(redis_srv, device_map, create_worker, config)
# ddb.start()


def get_worker(iot_device):
	worker = device_map.get(iot_device)
	if not worker:
		db = redis_db.get(iot_device)
		if not db:
			db = "example"
		worker = create_worker(db)
		device_map[iot_device] = worker
	return worker


def get_input_type(val):
	if isinstance(val, int):
		return "int"
	elif isinstance(val, float):
		return "float"
	else:
		return "string"


inputs_map = {}


def get_input_vt(iot_device, device, input, val):
	if get_input_type(val) == "string":
		return "string", val

	key = iot_device + "/" + device + "/" + input
	vt = inputs_map.get(key)

	if vt == 'int':
		return vt, int(val)
	elif vt == 'string':
		return vt, str(val)
	else:
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
	logging.info("Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data")
	client.subscribe("+/data_gz")
	client.subscribe("+/apps")
	client.subscribe("+/apps_gz")
	client.subscribe("+/exts")
	client.subscribe("+/exts_gz")
	client.subscribe("+/devices")
	client.subscribe("+/devices_gz")
	client.subscribe("+/status")
	client.subscribe("+/stat")
	client.subscribe("+/stat_gz")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.error("Disconnect with result code "+str(rc))


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
		if not payload:
			logging.warning('Decode DATA JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return
		g = match_data_path.match(payload[0])
		if g and msg.retain == 0:
			g = g.groups()
			worker = get_worker(devid)
			prop = g[2]
			value=payload[2]
			if prop == "value":
				t, val = get_input_vt(devid, g[0], g[1], value)
				if t:
					prop = t + "_" + prop
				value = val
			else:
				value = str(value)
			# logging.debug('[GZ]device: %s\tInput: %s\t Value: %s', g[0], g[1], value)
			worker.append_data(name=g[1], property=prop, device=g[0], iot=devid, timestamp=payload[1], value=value, quality=payload[3])
		return

	if topic == 'data_gz':
		try:
			payload = zlib.decompress(msg.payload).decode('utf-8')
			data_list = json.loads(payload)
			if not data_list:
				logging.warning('Decode DATA_GZ JSON Failure: %s/%s\t%s', devid, topic, payload)
				return
			for d in data_list:
				g = match_data_path.match(d[0])
				if g and msg.retain == 0:
					g = g.groups()
					worker = get_worker(devid)
					prop = g[2]
					value = d[2]
					if prop == "value":
						t, val = get_input_vt(devid, g[0], g[1], value)
						if t:
							prop = t + "_" + prop
						value = val
					else:
						value = str(value)
					# logging.debug('[GZ]device: %s\tInput: %s\t Value: %s', g[0], g[1], value)
					worker.append_data(name=g[1], property=prop, device=g[0], iot=devid, timestamp=d[1], value=value, quality=d[3])
		except Exception as ex:
			logging.exception(ex)
			logging.debug('Catch an exception: %s\t%d\t%d', msg.topic, msg.qos, msg.retain)
		return

	if topic == 'apps' or topic == 'apps_gz':
		data = msg.payload.decode('utf-8') if topic == 'apps' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', devid, topic, data)
		worker = get_worker(devid)
		worker.append_data(name="iot_device", property="apps", device=devid, iot=devid, timestamp=time.time(), value=data, quality=0)

	if topic == 'exts' or topic == 'exts_gz':
		data = msg.payload.decode('utf-8') if topic == 'exts' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', devid, topic, data)
		worker = get_worker(devid)
		worker.append_data(name="iot_device", property="exts", device=devid, iot=devid, timestamp=time.time(), value=data, quality=0)

	if topic == 'devices' or topic == 'devices_gz':
		data = msg.payload.decode('utf-8') if topic == 'devices' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', devid, topic, data)
		worker = get_worker(devid)
		worker.append_data(name="iot_device", property="cfg", device=devid, iot=devid, timestamp=time.time(), value=data, quality=0)
		devs = json.loads(data)
		if not devs:
			logging.warning('Decode DEVICE_GZ JSON Failure: %s/%s\t%s', devid, topic, data)
			return
		make_input_map(devid, devs)
		return

	if topic == 'status':
		# TODO: Update Quality of All Inputs when gate is offline.
		worker = get_worker(devid)
		#redis_sts.set(devid, msg.payload.decode('utf-8'))
		status = msg.payload.decode('utf-8')
		if status == "ONLINE" or status == "OFFLINE":
			val = status == "ONLINE"
			worker.append_data(name="device_status", property="online", device=devid, iot=devid, timestamp=time.time(), value=val, quality=0)
		return

	if topic == 'stat':
		payload = json.loads(msg.payload.decode('utf-8'))
		if not payload:
			logging.warning('Decode STAT JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return
		g = match_stat_path.match(payload[0])
		if g and msg.retain == 0:
			g = g.groups()
			worker = get_worker(devid)
			value=float(payload[2])
			worker.append_data(name='_stat_'+g[1], property=g[2], device=g[0], iot=devid, timestamp=payload[1], value=value, quality=0)
		return

	if topic == 'stat_gz':
		try:
			payload = zlib.decompress(msg.payload).decode('utf-8')
			stat_list = json.loads(payload)
			if not stat_list:
				logging.warning('Decode STAT_GZ JSON Failure: %s/%s\t%s', devid, topic, payload)
				return
			for d in stat_list:
				g = match_stat_path.match(d[0])
				if g and msg.retain == 0:
					g = g.groups()
					worker = get_worker(devid)
					value=float(d[2])
					worker.append_data(name='_stat_'+g[1], property=g[2], device=g[0], iot=devid, timestamp=d[1], value=value, quality=0)
		except Exception as ex:
			logging.exception(ex)
			logging.debug('Catch an exception: %s\t%d\t%d', msg.topic, msg.qos, msg.retain)
		return

	if topic == 'event':
		payload = json.loads(msg.payload.decode('utf-8'))
		if not payload:
			logging.warning('Decode EVENT JSON Failure: %s/%s\t%s', devid, topic, msg.payload.decode('utf-8'))
			return
		if msg.retain == 0:
			worker = get_worker(devid)
			devsn = payload[0]
			event = payload[1]
			timestamp = payload[2] or time.time()
			worker.append_event(device=devsn, iot=devid, timestamp=timestamp, event=event, quality=0)
		return


client = mqtt.Client(client_id="SYS_MQTT_TO_INFLUXDB")
client.username_pw_set("root", "bXF0dF9pb3RfYWRtaW4K")
client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.on_message = on_message

mqtt_host = config.get('mqtt', 'host', fallback='127.0.0.1')
mqtt_port = config.getint('mqtt', 'port', fallback=1883)
mqtt_keepalive = config.getint('mqtt', 'port', fallback=60)

try:
	logging.debug('MQTT Connect to %s:%d', mqtt_host, mqtt_port)
	client.connect_async(mqtt_host, mqtt_port, mqtt_keepalive)

	# Blocking call that processes network traffic, dispatches callbacks and
	# handles reconnecting.
	# Other loop*() functions are available that give a threaded interface and a
	# manual interface.
	client.loop_forever(retry_first_connection=True)
except Exception as ex:
	logging.exception(ex)
	os._exit(1)

