import threading
import queue
import requests
import json
import logging
import datetime
import time
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
		logging.debug("TaskBase")


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
				logging.error("This is empty Exeption!")
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

	def device_event(self, *args, **kwargs):
		self.add(DeviceEvent(*args, **kwargs))


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
			logging.warning(r.text)


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
			logging.warning(r.text)


class UpdateDeviceStatus(TaskBase):
	def __init__(self, sn, status):
		self.sn = sn
		self.status = status

	def run(self):
		session = requests.session()
		init_request_headers(session.headers)

		r = session.post(api_srv + ".update_device_status", data=json.dumps({"sn": self.sn,	"status": self.status}))
		if r.status_code != 200:
			logging.warning(r.text)

DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S.%f"
DATETIME_FORMAT = DATE_FORMAT + " " + TIME_FORMAT

class DeviceEvent(TaskBase):
	def __init__(self, sn, event):
		self.sn = sn
		self.event = event

	def run(self):
		session = requests.session()
		init_request_headers(session.headers)
		event = json.loads(self.event)
		timestamp = datetime.datetime.utcfromtimestamp(event[2]).strftime(DATETIME_FORMAT)

		data= json.dumps({
			"device": self.sn,
			"level": event[1].get("level") or 0,
			"type": event[1].get("type") or "EVENT",
			"info": event[1].get("info") or "EVENT INFO",
			"data": json.dumps(event[1].get("data")),
			"time": timestamp,
			"wechat_notify": 1,
		})

		r = session.post(api_srv + ".add_device_event", data=data)
		if r.status_code != 200:
			logging.warning(r.text)