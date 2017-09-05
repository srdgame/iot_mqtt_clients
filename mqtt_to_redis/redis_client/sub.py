'''
Publish/Subscribe message broker between Redis and MQTT
'''
import threading
import redis
import json
import re
import os
import logging
import paho.mqtt.client as mqtt


match_result = re.compile(r'^([^/]+)/result/([^/]+)')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Sub MQTT Connected with result code "+str(rc))
	client.subscribe("+/result/#")


def on_disconnect(client, userdata, rc):
	logging.info("Sub MQTT Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_result.match(msg.topic)
	if g:
		g = g.groups()
		dev = g[0]
		action = g[1]
		userdata.on_mqtt_message(dev, action, msg.payload.decode('utf-8'))


class MQTTClient(threading.Thread):
	def __init__(self, client, host="localhost", port=1883, keepalive=60):
		threading.Thread.__init__(self)
		self.client = client
		self.host = host
		self.port = port
		self.keepalive = keepalive

	def run(self):
		try:
			mqttc = mqtt.Client(userdata=self.client, client_id="SYS_MQTT_TO_REDIS.SUB")
			mqttc.username_pw_set("root", "bXF0dF9pb3RfYWRtaW4K")
			self.mqttc = mqttc

			mqttc.on_connect = on_connect
			mqttc.on_disconnect = on_disconnect
			mqttc.on_message = on_message

			logging.debug('MQTT Connect to %s:%d', self.host, self.port)
			mqttc.connect_async(self.host, self.port, self.keepalive)

			mqttc.loop_forever(retry_first_connection=True)
		except Exception as ex:
			logging.exception('MQTT Exeption')
			os._exit(1)

	def publish(self, *args, **kwargs):
		return self.mqttc.publish(*args, **kwargs)


class SubClient(threading.Thread):
	def __init__(self, srv, config):
		threading.Thread.__init__(self)
		self.srv = srv
		self.config = config

	def run(self):
		host = self.config.get('mqtt', 'host', fallback='127.0.0.1')
		port = self.config.getint('mqtt', 'port', fallback=1883)
		keepalive = self.config.getint('mqtt', 'keepalive', fallback=60)
		mqttc = MQTTClient(self, host=host, port=port, keepalive=keepalive)
		mqttc.start()
		self.mqttc = mqttc

		redis_client = redis.Redis.from_url(self.srv + "/7")
		ps = redis_client.pubsub()
		ps.subscribe(['device_app', 'device_sys', 'device_output', 'device_command'])
		self.redis_client = redis_client
		self.pubsub = ps

		for item in ps.listen():
			if item['type'] == 'message':
				self.on_redis_message(item['channel'].decode('utf-8'), item['data'].decode('utf-8'))

	def on_redis_message(self, channel, msg):
		try:
			'''
			Forward redis publish message to mqtt broker
			'''
			logging.debug('redis_message\t%s\t%s', channel, msg)
			request = json.loads(msg)
			topic = request['device'] + "/" + channel[7:]
			if request.get('topic'):
				topic = topic + "/" + request['topic']
				request.pop('topic')
			if request.get('payload'):
				request = request.get('payload')
			else:
				request = json.dumps(request)
			r = self.mqttc.publish(topic=topic, payload=request, qos=1, retain=False)
			logging.debug("Sub MQTT publish result: " + str(r))
		except Exception as ex:
			logging.exception('Catch an exception.')

	def on_mqtt_message(self, dev, action, msg):
		try:
			'''
			Forward mqtt publish action result to redis
			'''
			logging.debug('mqtt_message\t%s\t%s\t%s', dev, action, msg)
			result = json.loads(msg)
			if not result.get('device'):
				result['device'] = dev
			r = self.redis_client.publish("device_" + action + "_result", json.dumps(result))
			logging.debug("Sub Redis publish result: " + str(r))
			if result.get('id'):
				r = self.redis_client.set(result['id'], json.dumps(result), 600)
				logging.debug(str(r))
		except Exception as ex:
			logging.exception('Catch an exception.')
