import threading
import queue
import requests
import json
from configparser import ConfigParser


config = ConfigParser()
config.read('../config.ini')
api_srv = config.get('frappe', 'url', fallback='http://127.0.0.1:8000') + "/api/method/iot.hdb_api"

def init_request_headers(headers):
	headers['HDB-AuthorizationCode'] = '12312313aaa'
	headers['Content-Type'] = 'application/json'
	headers['Accept'] = 'application/json'


class TaskBase:
	def run(self):
		print("TaskBase")


class Worker(threading.Thread):
	def __init__(self):
		threading.Thread.__init__(self)
		self.queue = queue.Queue()
		self.thread_stop = False

	def run(self):
		q = self.queue
		while not self.thread_stop:
			try:
				task = q.get()
				task.run()
				q.task_done()
			except queue.Empty:
				print("This is empty Exeption!")
				break

	def stop(self):
		self.thread_stop = True

	def add(self, task):
		self.queue.put(task)

	def create_device(self, *args, **kwargs):
		self.add(CreateDevice(*args, **kwargs))

	def update_device(self, *args, **kwargs):
		self.add(UpdateDevice(*args, **kwargs))

	def update_device_status(self, *args, **kwargs):
		self.add(UpdateDeviceStatus(*args, **kwargs))


class CreateDevice(TaskBase):
	def __init__(self, sn, props):
		self.sn = sn
		self.props = props

	def run(self):
		session = requests.session()
		#session.auth = (username, passwd)
		init_request_headers(session.headers)

		dev = {
			"sn": self.sn,
			"props": self.props,
		}
		r = session.post(api_srv + ".add_device", data=json.dumps(dev))
		if r.status_code != 200:
			print(r.text)


class UpdateDevice(TaskBase):
	def __init__(self, sn, props, status=None):
		self.sn = sn
		self.props = props
		self.status = status or "OFFLINE"

	def run(self):
		session = requests.session()
		#session.auth = (username, passwd)
		init_request_headers(session.headers)

		dev = {
			"sn": self.sn,
			"props": self.props,
			"status": self.status
		}
		r = session.post(api_srv + ".update_device", data=json.dumps(dev))
		if r.status_code != 200:
			print(r.text)


class UpdateDeviceStatus(TaskBase):
	def __init__(self, sn, status):
		self.sn = sn
		self.status = status

	def run(self):
		session = requests.session()
		init_request_headers(session.headers)

		r = session.post(api_srv + ".update_device_status", data=json.dumps({"sn": self.sn,	"status": self.status}))
		if r.status_code != 200:
			print(r.text)