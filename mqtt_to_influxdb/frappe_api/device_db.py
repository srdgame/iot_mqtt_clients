import threading
import time
import requests
import redis
import logging


def init_request_headers(headers, auth_code):
	headers['HDB-AuthorizationCode'] = auth_code or '12312313aaa'
	#headers['Content-Type'] = 'application/json'
	headers['Accept'] = 'application/json'


class DeviceDB(threading.Thread):
	def __init__(self, redis_srv, device_map, create_worker, config):
		threading.Thread.__init__(self)
		self.thread_stop = False
		self.device_map = device_map
		self.create_worker = create_worker

		self.redis_db = redis.Redis.from_url(redis_srv + "/8", decode_responses=True) # device influxdb database
		self.api_srv = config.get('frappe', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.hdb_api"
		self.auth_code = config.get('frappe', 'auth_code', fallback='12312313aaa')

		session = requests.session()
		# session.auth = (username, passwd)
		init_request_headers(session.headers, self.auth_code)
		self.session = session

	def run(self):
		device_map = self.device_map
		create_worker = self.create_worker
		redis_db = self.redis_db
		time.sleep(5)
		while not self.thread_stop:
			try:
				for device in self.device_map:
					db = self.get_db(device)
					redis_db.set(device, db)
					device_map[device] = create_worker(db)

			except Exception as ex:
				logging.exception(ex)

			time.sleep(3 * 60)

	def stop(self):
		self.thread_stop = True

	def get_db(self, device):
		r = self.session.get(self.api_srv + ".get_device_db", params={"sn": device})
		if r.status_code != 200:
			logging.error(r.text)
			raise r.status_code
		db = r.json()
		logging.debug('%s\t%s\t%s', str(time.time()), device, db)
		return db.get("message") or "example"

