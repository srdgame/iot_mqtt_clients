import threading
import redis
import json
import re
import paho.mqtt.client as mqtt


match_result = re.compile(r'^([^/]+)/([^/]+)/result')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Connected with result code "+str(rc))
	client.subscribe("+/app/result")
	client.subscribe("+/sys/result")
	client.subscribe("+/output/result")
	client.subscribe("+/command/result")


def on_disconnect(client, userdata, rc):
	print("Disconnect with result code "+str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	g = match_result.match(msg.topic)
	if g:
		g = g.groups()
		dev = g[0]
		action = g[1]
		userdata.on_mqtt_message(dev, action, msg.payload.decode('utf-8'))


class MqttClient(threading.Thread):
	def __init__(self, client, host="localhost", port=1883, keepalive=60):
		threading.Thread.__init__(self)
		self.client = client
		self.host = host
		self.port = port
		self.keepalive = keepalive

	def run(self):
		mqttc = mqtt.Client(userdata=self.client)
		self.mqttc = mqttc

		mqttc.on_connect = on_connect
		mqttc.on_disconnect = on_disconnect
		mqttc.on_message = on_message

		mqttc.connect(self.host, self.port, self.keepalive)

		while True:
			mqttc.loop()

	def publish(self, *args, **kwargs):
		return self.mqttc.publish(*args, **kwargs)


class SubClient(threading.Thread):
	def __init__(self, srv):
		threading.Thread.__init__(self)
		self.srv = srv

	def run(self):
		mqttc = MqttClient(self)
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

	def on_redis_message(self, channel, str):
		try:
			print('redis_message', channel, str)
			request = json.loads(str)
			topic = request['device'] + "/" + channel[7:]
			if request.get('topic'):
				topic = topic + "/" + request['topic']
			r = self.mqttc.publish(topic=topic, payload=str, qos=1, retain=False)
			print(r)
		except Exception as ex:
			print(ex)

	def on_mqtt_message(self, dev, action, str):
		try:
			print('mqtt_message', dev, action, str)
			result = json.loads(str)
			if not result.get('device'):
				result['device'] = dev
			r = self.redis_client.publish("device_" + action + "_result", json.dumps(result))
			print(r)
			if result.get('id'):
				r = self.redis_client.set(result['id'], json.dumps(result), 600)
				print(r)
		except Exception as ex:
			print(ex)