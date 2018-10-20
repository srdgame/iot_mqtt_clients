
from __future__ import unicode_literals
import re
import os
import json
import redis
import logging
import zlib
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
redis_exts = redis.Redis.from_url(redis_srv+"/5", decode_responses=True) # device installed extension list
redis_apps = redis.Redis.from_url(redis_srv+"/6", decode_responses=True) # device installed application list
#redis_result = redis.Redis.from_url(redis_srv+"/7", decode_responses=True) # device command/batch result
redis_sts = redis.Redis.from_url(redis_srv+"/9", decode_responses=True) # device status (online or offline)
redis_cfg = redis.Redis.from_url(redis_srv+"/10", decode_responses=True) # device defines
redis_rel = redis.Redis.from_url(redis_srv+"/11", decode_responses=True) # device relationship
redis_rtdb = redis.Redis.from_url(redis_srv+"/12", decode_responses=True) # device real-time data

''' Set all data be expired after device offline '''
redis_offline_expire = 3600 * 24 * 7

match_topic = re.compile(r'^([^/]+)/(.+)$')
match_data_path = re.compile(r'^([^/]+)/(.+)$')


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Main MQTT Connected with result code "+str(rc))

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

	gateid = g[0]
	topic = g[1]

	if topic == 'data':
		data = json.loads(msg.payload.decode('utf-8'))
		if not data:
			logging.warning('Decode DATA JSON Failure: %s/%s\t%s', gateid, topic, msg.payload.decode('utf-8'))
			return
		g = match_data_path.match(data[0])
		if g and msg.retain == 0:
			g = g.groups()
			dev = g[0]
			intput = g[1]
			# pop input key
			data.pop(0)

			# logging.debug('device: %s\tInput: %s\t Value: %s', g[0], g[1], json.dumps(payload))
			r = redis_rtdb.hmset(dev, {
				intput: json.dumps(data)
			})
		return

	if topic == 'data_gz':
		try:
			data = zlib.decompress(msg.payload).decode('utf-8')
			data_list = json.loads(data)
			if not data_list:
				logging.warning('Decode DATA_GZ JSON Failure: %s/%s\t%s', gateid, topic, data)
				return
			for d in data_list:
				g = match_data_path.match(d[0])
				if g and msg.retain == 0:
					g = g.groups()
					dev = g[0]
					intput = g[1]
					# pop input key
					d.pop(0)

					# logging.debug('device: %s\tInput: %s\t Value: %s', g[0], g[1], json.dumps(d))
					r = redis_rtdb.hmset(dev, {
						intput: json.dumps(d)
					})
		except Exception as ex:
			logging.exception(ex)
			logging.debug('Catch an exception: %s\t%d\t%d', msg.topic, msg.qos, msg.retain)
		return

	if topic == 'apps' or topic == 'apps_gz':
		data = msg.payload.decode('utf-8') if topic == 'apps' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', gateid, topic, data)
		# apps = json.loads(data)
		# redis_apps.set(gateid, json.dumps(apps))
		redis_apps.set(gateid, data)
		return

	if topic == 'exts' or topic == 'exts_gz':
		data = msg.payload.decode('utf-8') if topic == 'exts' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', gateid, topic, data)
		# exts = json.loads(data)
		# redis_exts.set(gateid, json.dumps(exts))
		redis_exts.set(gateid, data)
		return

	if topic == 'devices' or topic == 'devices_gz':
		data = msg.payload.decode('utf-8') if topic == 'devices' else zlib.decompress(msg.payload).decode('utf-8')
		logging.debug('%s/%s\t%s', gateid, topic, data)
		devs = json.loads(data)
		if not devs:
			logging.warning('Decode DEVICE_GZ JSON Failure: %s/%s\t%s', gateid, topic, data)
			return

		devkeys = redis_rel.lrange(gateid, 0, 1000)
		redis_rel.ltrim(gateid, 0, -1000)

		## Cleanup cfg and rtdb
		for devid in devkeys:
			if devs.get(devid) is None:
				redis_cfg.expire(devid, redis_offline_expire)
				redis_rtdb.expire(devid, redis_offline_expire)
				redis_rel.expire('PARENT_{0}'.format(devid), redis_offline_expire)

		for devid in devs:
			redis_cfg.persist(devid)
			redis_cfg.set(devid, json.dumps(devs[devid]))

			redis_rel.lpush(gateid, devid)

			redis_rtdb.persist(devid)

			redis_rel.persist('PARENT_{0}'.format(devid))
			redis_rel.set('PARENT_{0}'.format(devid), gateid)

		return

	if topic == 'status':
		status = msg.payload.decode('utf-8')
		redis_sts.set(gateid, status)
		worker.update_device_status(gateid, status)
		if status == 'OFFLINE':
			redis_sts.expire(gateid, redis_offline_expire)
			redis_rel.expire(gateid, redis_offline_expire)
			redis_apps.expire(gateid, redis_offline_expire)
			redis_exts.expire(gateid, redis_offline_expire)
			devkeys = redis_rel.lrange(gateid, 0, 1000)
			for devid in devkeys:
				redis_cfg.expire(devid, redis_offline_expire)
				redis_rtdb.expire(devid, redis_offline_expire)
				redis_rel.expire('PARENT_{0}'.format(devid), redis_offline_expire)
		else:
			redis_sts.persist(gateid)
			redis_rel.persist(gateid)
			redis_apps.persist(gateid)
			redis_exts.persist(gateid)

		return

	if topic == 'event':
		event = msg.payload.decode('utf-8')
		worker.device_event(gateid, event)
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
	logging.exception(ex)
	os._exit(1)
