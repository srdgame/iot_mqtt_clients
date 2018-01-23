
from __future__ import unicode_literals
import influxdb
from influxdb.exceptions import InfluxDBClientError
import logging


class Client:
	def __init__(	self, host, port, username, password, database):
		self.host = host
		self.port = port
		self.username = username
		self.password = password
		self.database = database
		self._client = None

	def connect(self):
		self._client = influxdb.InfluxDBClient( host=self.host,
												port=self.port,
												username=self.username,
												password=self.password,
												database=self.database )

	def write_data(self, data_list):
		points = []
		for data in data_list:
			if data.get('level') is not None and data['name'] == 'iot_device_event':
				points.append({
					"measurement": data['name'],
					"tags": {
						"device": data['device'],
						"iot": data['iot']
					},
					"time": int(data['timestamp'] * 1000),
					"fields": {
						data['property']: data['value'],
						"quality": data['quality'],
						"level": data['level']
					}
				})
			else:
				points.append({
					"measurement": data['name'],
					"tags": {
						"device": data['device'],
						"iot": data['iot']
					},
					"time": int(data['timestamp'] * 1000),
					"fields": {
						data['property']: data['value'],
						"quality": data['quality'],
					}
				})
		try:
			self._client.write_points(points, time_precision='ms')
		except InfluxDBClientError as ex:
			if ex.code == 400:
				logging.exception('Catch an exception.')
				return

	def create_database(self):
		try:
			self._client.create_database(self.database)
		except Exception as ex:
			logging.exception('Catch an exception.')