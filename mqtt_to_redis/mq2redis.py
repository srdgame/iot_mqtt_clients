
from __future__ import unicode_literals
import re
import os
import json
import redis
import logging
from collections import deque
from configparser import ConfigParser
import paho.mqtt.client as mqtt
from frappe_api.worker import Worker
from redis_client.sub import SubClient


logging.basicConfig(level=logging.DEBUG,
                format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                datefmt='%a, %d %b %Y %H:%M:%S')

config = ConfigParser()
config.read('../config.ini')

redis_srv = config.get('redis', 'url', fallback='redis://127.0.0.1:6379')
redis_exts = redis.Redis.from_url(redis_srv+"/5") # device installed extension list
redis_apps = redis.Redis.from_url(redis_srv+"/6") # device installed application list
redis_sts = redis.Redis.from_url(redis_srv+"/9") # device status (online or offline)
redis_cfg = redis.Redis.from_url(redis_srv+"/10") # device defines
redis_rel = redis.Redis.from_url(redis_srv+"/11") # device relationship
redis_rtdb = redis.Redis.from_url(redis_srv+"/12") # device real-time data

''' Set all data be expired after device offline '''
redis_offline_expire = 3600 * 24 * 7

data_queue = deque()
match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')

device_status = {}


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Main MQTT Connected with result code "+str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	client.subscribe("+/data")
	client.subscribe("+/apps")
	client.subscribe("+/exts")
	client.subscribe("+/devices")
	client.subscribe("+/status")
	client.subscribe("+/event")


def on_disconnect(client, userdata, rc):
	logging.info("Main MQTT Disconnect with result code "+str(rc))


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
			dev = g[0]
			intput = g[1]
			# pop input key
			payload.pop(0)

			ttl = redis_rtdb.ttl(dev)
			if ttl and (ttl >= 0):
				redis_rtdb.persist(dev)

			# logging.debug('device: %s\tInput: %s\t Value: %s', g[0], g[1], json.dumps(payload))
			r = redis_rtdb.hmset(dev, {
				intput: json.dumps(payload)
			})
		return

	if topic == 'apps':
		apps = json.loads(msg.payload.decode('utf-8'))
		logging.debug('%s/apps\t%s', devid, str(apps))
		redis_apps.set(devid, json.dumps(apps))

	if topic == 'exts':
		apps = json.loads(msg.payload.decode('utf-8'))
		logging.debug('%s/exts\t%s', devid, str(apps))
		redis_exts.set(devid, json.dumps(apps))

	if topic == 'devices':
		devs = json.loads(msg.payload.decode('utf-8'))
		logging.debug('%s/devices\t%s', devid, str(devs))

		ttl = redis_rel.ttl(devid)
		if ttl and (ttl >= 0):
			redis_rel.persist(devid)
		devkeys = redis_rel.lrange(devid, 0, 1000)
		redis_rel.ltrim(devid, 0, -1000)

		## Cleanup cfg and rtdb
		for dev in devkeys:
			if devs.get(dev) is None:
				redis_cfg.expire(dev, redis_offline_expire)
				redis_rtdb.expire(dev, redis_offline_expire)

		for dev in devs:
			redis_cfg.set(dev, json.dumps(devs[dev]))
			redis_rel.lpush(devid, dev)
			''' MQTT authed by frappe's IOT Device, so we do not need to create device
			if dev == devid:
				worker.create_device(devid, devs[devid])
			'''
		return

	if topic == 'status':
		status = msg.payload.decode('utf-8')
		redis_sts.set(devid, status)

		device_status[devid] = status
		worker.update_device_status(devid, status)
		if status == 'OFFLINE':
			redis_sts.expire(devid, redis_offline_expire)
			redis_rel.expire(devid, redis_offline_expire)
			redis_apps.expire(devid, redis_offline_expire)
			redis_exts.expire(devid, redis_offline_expire)
			devkeys = redis_rel.lrange(devid, 0, 1000)
			for dev in devkeys:
				redis_cfg.expire(dev, redis_offline_expire)
				redis_rtdb.expire(dev, redis_offline_expire)

		return

	if topic == 'event':
		event = msg.payload.decode('utf-8')
		worker.device_event(devid, event)
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
mqtt_port = config.getint('mqtt', 'port', fallback=1883)
mqtt_keepalive = config.getint('mqtt', 'keepalive', fallback=60)

try:
	logging.debug('MQTT Connect to %s:%d', mqtt_host, mqtt_port)
	client.connect_async(mqtt_host, mqtt_port, mqtt_keepalive)

	# Blocking call that processes network traffic, dispatches callbacks and
	# handles reconnecting.
	# Other loop*() functions are available that give a threaded interface and a
	# manual interface.
	client.loop_forever(retry_first_connection=True)
except Exception as ex:
	logging.exception('MQTT Exeption')
	os._exit(1)
