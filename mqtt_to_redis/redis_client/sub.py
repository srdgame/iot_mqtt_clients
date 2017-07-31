'''
Publish/Subscribe message broker between Redis and MQTT
'''
import threading
import redis
import json
import re
import paho.mqtt.client as mqtt


match_result = re.compile(r'^([^/]+)/result/([^/]+)')

# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	print("Sub MQTT Connected with result code "+str(rc))
	client.subscribe("+/result/app")
	client.subscribe("+/result/sys")
	client.subscribe("+/result/output")
	client.subscribe("+/result/command")


def on_disconnect(client, userdata, rc):
	print("Sub MQTT Disconnect with result code "+str(rc))


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
		mqttc = mqtt.Client(userdata=self.client, client_id="SYS_MQTT_TO_REDIS.SUB")
		mqttc.username_pw_set("root", "root")
		self.mqttc = mqttc

		mqttc.on_connect = on_connect
		mqttc.on_disconnect = on_disconnect
		mqttc.on_message = on_message

		mqttc.connect(self.host, self.port, self.keepalive)

		mqttc.loop_forever()

	def publish(self, *args, **kwargs):
		return self.mqttc.publish(*args, **kwargs)


class SubClient(threading.Thread):
	def __init__(self, srv):
		threading.Thread.__init__(self)
		self.srv = srv

	def run(self):
		mqttc = MQTTClient(self)
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
			'''
			Forward redis publish message to mqtt broker
			'''
			print('redis_message', channel, str)
			request = json.loads(str)
			topic = request['device'] + "/" + channel[7:]
			if request.get('topic'):
				topic = topic + "/" + request['topic']
				request.pop('topic')
			if request.get('payload'):
				request = request.get('payload')
			else:
				request = json.dumps(request)
			r = self.mqttc.publish(topic=topic, payload=request, qos=1, retain=False)
			print("Sub MQTT publish result: ", r)
		except Exception as ex:
			print(ex)

	def on_mqtt_message(self, dev, action, str):
		try:
			'''
			Forward mqtt publish action result to redis
			'''
			print('mqtt_message', dev, action, str)
			result = json.loads(str)
			if not result.get('device'):
				result['device'] = dev
			r = self.redis_client.publish("device_" + action + "_result", json.dumps(result))
			print("Sub Redis publish result: ", r)
			if result.get('id'):
				r = self.redis_client.set(result['id'], json.dumps(result), 600)
				print(r)
		except Exception as ex:
			print(ex)
