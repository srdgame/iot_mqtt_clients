
from __future__ import unicode_literals
import influxdb


class Client:
	def __init__(	self,
					host='localhost',
					port=8086,
					username='root',
					password='root',
					database='example'	):
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
		self._client.write_points(points, time_precision='ms')

	def create_database(self):
		try:
			self._client.create_database(self.database)
		except Exception as ex:
			print(ex)