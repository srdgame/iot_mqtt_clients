import threading
import time
import requests
import redis


api_srv = "http://127.0.0.1:8000/api/method/iot.hdb_api"


def init_request_headers(headers):
	headers['HDB-AuthorizationCode'] = '12312313aaa'
	headers['Content-Type'] = 'application/json'
	headers['Accept'] = 'application/json'


class DeviceDB(threading.Thread):
	def __init__(self, redis_srv, device_map, create_worker):
		threading.Thread.__init__(self)
		self.thread_stop = False
		self.device_map = device_map
		self.create_worker = create_worker

		session = requests.session()
		# session.auth = (username, passwd)
		init_request_headers(session.headers)
		self.session = session
		self.redis_db = redis.Redis.from_url(redis_srv + "/8")

	def run(self):
		device_map = self.device_map
		create_worker = self.create_worker
		redis_db = self.redis_db
		time.sleep(5)
		while not self.thread_stop:
			try:
				for device in self.device_map:
					db = self.get_db(device)
					device_map[device] = create_worker(db)
					redis_db.set(device, db)

			except Exception as ex:
				print(ex)
				break

			time.sleep(30)

	def stop(self):
		self.thread_stop = True

	def get_db(self, device):
		r = self.session.get(api_srv + ".get_device_db", params={"sn": device})
		if r.status_code != 200:
			print(r.text)
		db = r.json()
		print(time.time(), device, db)
		return db.get("message") or "example"

