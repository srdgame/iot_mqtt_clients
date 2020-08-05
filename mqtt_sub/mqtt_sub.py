
from __future__ import unicode_literals
import re
import sys
import base64
import json
import binascii
import logging
import zlib
import paho.mqtt.client as mqtt
from configparser import ConfigParser


console_out = logging.StreamHandler(sys.stdout)
console_out.setLevel(logging.DEBUG)
console_err = logging.StreamHandler(sys.stderr)
console_err.setLevel(logging.ERROR)
logging_handlers = [console_out, console_err]
logging_format = '%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s'
logging_datefmt = '%a, %d %b %Y %H:%M:%S'
logging.basicConfig(level=logging.DEBUG, format=logging_format, datefmt=logging_datefmt, handlers=logging_handlers)


config = ConfigParser()
config.read('../config.ini')


match_comm = re.compile(r'^([^/]+)/comm$')
match_xxx_gz = re.compile(r'^([^/]+)/[^/]+_gz')
match_status = re.compile(r'^([^/]+)/status$')


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
	logging.info("Main MQTT Connected with result code " + str(rc))

	# Subscribing in on_connect() means that if we lose the connection and
	# reconnect then subscriptions will be renewed.
	#client.subscribe("$SYS/#")
	logging.debug(str(client.subscribe("+/#")))
	#logging.debug(str(client.subscribe("IDIDIDIDID/app/#")))


def on_disconnect(client, userdata, rc):
	logging.error("Main MQTT Disconnect with result code " + str(rc))


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
	try:
		g = match_status.match(msg.topic)
		if g:
			logging.debug('%s\t%s\t%d\t%d', msg.topic, msg.payload.decode('utf-8', 'surrogatepass'), msg.qos, msg.retain)
			return
		g = match_xxx_gz.match(msg.topic)
		if g:
			val = zlib.decompress(msg.payload).decode('utf-8', 'surrogatepass')
			logging.debug('%s\t%s', msg.topic, str(val))
			return
		data = json.loads(msg.payload.decode('utf-8', 'surrogatepass'))
		g = match_comm.match(msg.topic)
		if g:
			raw = base64.b64decode(data[2])
			logging.debug('%s\t%s\t%s', str(data[0]), str(data[1]), binascii.b2a_hex(raw))
			return
		logging.debug('%s\t%s\t%d\t%d', msg.topic, str(data), msg.qos, msg.retain)
	except Exception as ex:
		logging.exception(ex)
		logging.debug('Catch an exception: %s\t%s\t%d\t%d', msg.topic, msg.payload.decode('utf-8', 'surrogatepass'), msg.qos, msg.retain)


def on_subscribe(client, userdata, mid, granted_qos):
	logging.info('ON_SUBSCRIBE %d %s', mid, str(granted_qos))


if __name__ == '__main__':

	# Listen on MQTT forwarding real-time data into redis, and forwarding configuration to frappe.
	client = mqtt.Client(client_id="SYS_MQTT_SUB_CLIENT")
	mqtt_user = config.get('mqtt', 'user', fallback="root")
	mqtt_password = config.get('mqtt', 'password', fallback="bXF0dF9pb3RfYWRtaW4K")
	client.username_pw_set(mqtt_user, mqtt_password)
	#client.username_pw_set("changch84@163.com", "pa88word")
	client.on_connect = on_connect
	client.on_disconnect = on_disconnect
	client.on_message = on_message
	client.on_subscribe = on_subscribe

	mqtt_host = config.get('mqtt', 'host', fallback='127.0.0.1')
	mqtt_port = config.getint('mqtt', 'port', fallback=1883)
	mqtt_keepalive = config.getint('mqtt', 'keepalive', fallback=60)
	client.connect(mqtt_host, mqtt_port, mqtt_keepalive)


	# Blocking call that processes network traffic, dispatches callbacks and
	# handles reconnecting.
	# Other loop*() functions are available that give a threaded interface and a
	# manual interface.
	client.loop_forever()
