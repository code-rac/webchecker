from model import *
import requests
import time
from pprint import pprint
import threading
import random

N_EPOCHS = 5  # request N_EPOCHS time each time
N_BATCHES = 10
JOBS = []
USER_URLS = []
INTERVAL = 100
LOCK = threading.Lock()
GAP = 1
SAFETY_PARAM = 5
USER_AGENTS = open('user-agents.txt').read().split('\n')

CACHE_EVENT_URL = {}
CACHE_START_EVENT = []
CACHE_URL_TIMESTAMP = {}

class Checker(threading.Thread):

    def __init__(self):
        threading.Thread.__init__(self)
        self.sess = requests.session()
        self.event = Event()
        self.datapoints = None
        self.url = Url()
        self.master_url = MasterUrl()

    def get_one_job(self):
        global JOBS
        job = (None, None)
        LOCK.acquire()
        if JOBS:
            job = JOBS.pop(0)
        LOCK.release()
        return job

    def datapoint_generator(self, url_id, url):
        self.datapoints = []
        for datapoint in self.request(url):
            self.datapoints.append(datapoint)
            for _url_id, _user_id in USER_URLS:
                if _url_id == url_id:
                    metadata = {
                        'user_id': _user_id,
                        'url_id': _url_id,
                        '_index': 'webassistant3',
                        '_type': 'datapoint'
                    }
                    metadata.update(datapoint)
                    yield metadata

    def calculate_event(self, url_id):
        global CACHE_EVENT_URL
        assert len(self.datapoints) > 0 and len(self.datapoints) <= N_EPOCHS

        last_datapoint = self.datapoints[len(self.datapoints)-1]
        timestamp = time.time()

        if url_id in CACHE_EVENT_URL.keys():
            if CACHE_EVENT_URL[url_id]['end_status_code'] != last_datapoint['status_code']:
                metadata = {
                    'end_status_code': CACHE_EVENT_URL[url_id]['end_status_code'],
                    'start_status_code': last_datapoint['status_code'],
                    'time_response' : last_datapoint['time_response'],
                    'prev_duration': timestamp - CACHE_EVENT_URL[url_id]['end_timestamp'],
                    'duration': 0,
                    'timestamp': timestamp,
                    'screenshot': None,
                    'type': None
                }
                CACHE_EVENT_URL[url_id].update({
                    'end_status_code': last_datapoint['status_code'],
                    'end_timestamp': timestamp
                })
                if metadata['start_status_code'] == 200:
                    self.url.update(1, url_id, last_datapoint['time_response'], metadata['start_status_code'])
                else:
                    self.url.update(0, url_id, last_datapoint['time_response'], metadata['start_status_code'])
                      
                if metadata['start_status_code'] < 400:
                    metadata['type'] = 'Up'
                else:
                    metadata['type'] = 'Down'
                return [metadata]
            else:
                self.url.update_time_response(url_id, last_datapoint['time_response'])
                return []
        else:
            metadata = {
                'end_status_code': None,
                'start_status_code': last_datapoint['status_code'],
                'time_response' : last_datapoint['time_response'],
                'duration': 0,
                'prev_duration': 0,
                'timestamp': timestamp,
                'screenshot': None,
                'type': None
            }
            CACHE_EVENT_URL[url_id] = {
                'end_status_code': last_datapoint['status_code'],
                'end_timestamp': timestamp
            }

            if metadata['start_status_code'] == 200:
                self.url.update(1, url_id, last_datapoint['time_response'], metadata['start_status_code'])
            else:
                self.url.update(0, url_id, last_datapoint['time_response'], metadata['start_status_code'])
                      
            if metadata['start_status_code'] < 400:
                metadata['type'] = 'Up'
            else:
                metadata['type'] = 'Down'
            return [metadata]
        return []

    def event_generator(self, url_id):
        for event in self.calculate_event(url_id):
            if event['prev_duration']:
                self.event.update_duration(url_id, event['prev_duration'])
            for _url_id, _user_id in USER_URLS:
                if _url_id == url_id:
                    metadata = {
                        'user_id': _user_id,
                        'url_id': _url_id,
                        '_index': 'webassistant3',
                        '_type': 'event'
                    }
                    metadata.update(event)
                    print(time.ctime(time.time())),
                    print(metadata)
                    yield metadata

    def assert_cache_url_timestamp(self, url_id, last_datapoint=None):
        if url_id not in CACHE_URL_TIMESTAMP:
            if not last_datapoint:
                last_datapoint = self.event.get_last_datapoint(url_id)
            if last_datapoint:
                CACHE_URL_TIMESTAMP.update({url_id: last_datapoint['timestamp']})

    def update_master_url_uptime(self, url_id):
        last_datapoint = self.datapoints[len(self.datapoints)-1]
        if last_datapoint['status_code'] < 400:
            timestamp = last_datapoint['timestamp']
            prev_timestamp = CACHE_URL_TIMESTAMP[url_id]
            CACHE_URL_TIMESTAMP[url_id] = timestamp
            self.master_url.increase_up_time(url_id, timestamp - prev_timestamp)

    def request(self, url):
        headers = {'User-agent': random.choice(USER_AGENTS)}
        for i in range(N_EPOCHS):
            try:
                r = self.sess.get(url, timeout=30, headers=headers)
            except:
                data = {
                    'time_response': None,
                    'status_code': 408,
                    'timestamp': time.time()
                }
            else:
                data = {
                    'time_response': r.elapsed.total_seconds(),
                    'status_code': r.status_code,
                    'timestamp': time.time()
                }
            finally:
                yield data
                if data['status_code'] == 200:
                    break
                time.sleep(0.1)

    def run(self):
        while 1:
            id, url = self.get_one_job()
            if id:
                self.assert_cache_url_timestamp(id)

                datapoint_generator = self.datapoint_generator(id, url)
                self.event.insert(datapoint_generator)

                self.assert_cache_url_timestamp(id, self.datapoints[len(self.datapoints)-1])
                self.update_master_url_uptime(id)

                event_generator = self.event_generator(id)
                self.event.insert(event_generator)
            time.sleep(GAP)

class WebChecker():

    def __init__(self):
        self.url = Url()
        self.event = Event()

    def decon(self):
        new_threads = int(self.url.count() / N_BATCHES * SAFETY_PARAM) - threading.active_count()
        if new_threads > 0:
            for i in range(new_threads):
                print('Start new thread', threading.active_count())
                thread = Checker()
                thread.daemon = True
                thread.start()

    def reschedule(self):
        global JOBS
        urls = self.url.get()  # return ((1, u'http://vnist.vn'), (2, u'https://beta.vntrip.vn'), (3, u'https://vinhphuc1000.vn'), (4, u'https://lab.vnist.vn'))
        len_urls = len(urls)
        start_at = time.time()
        for i in range(0, len_urls, N_BATCHES):
            LOCK.acquire()
            JOBS += urls[0:N_BATCHES]
            LOCK.release()
            urls = urls[N_BATCHES:]
            print('Number of jobs =', len(JOBS), 'Number of threads =', threading.active_count())
            time.sleep(INTERVAL / len_urls * N_BATCHES)

        if len(urls):
            LOCK.acquire()
            JOBS += urls
            LOCK.release()

    def load_user_urls(self):
        global USER_URLS
        USER_URLS = self.url.get_user_url()

    def load_start_events(self):
        global CACHE_START_EVENT
        CACHE_START_EVENT = list(self.event.get_start_events())
    
    def start_event_generator(self):
        for _url_id, _user_id in USER_URLS:
            flag = False
            for start_event in CACHE_START_EVENT:
                if start_event['url_id'] == _url_id and start_event['user_id'] == _user_id:
                    flag = True
                    break
            
            if not flag:
                metadata = {
                    'user_id': _user_id,
                    'url_id': _url_id,
                    '_index': 'webassistant3',
                    '_type': 'event',
                    'type' : 'Start',
                    'time_response': None,
                    'status_code': None,
                    'timestamp': time.time(),
                    'end_status_code': None,
                    'start_status_code': None,
                    'duration': -1,
                    'screenshot': None
                }
                CACHE_START_EVENT.append({'url_id': _url_id, 'user_id': _user_id})
                print(metadata)
                yield metadata

    def run(self):
        self.load_start_events()
        while 1:
            reload()
            self.load_user_urls()
            self.event.insert(self.start_event_generator())
            self.decon()
            self.reschedule()
            time.sleep(1)

if __name__ == '__main__':
    WebChecker().run()
