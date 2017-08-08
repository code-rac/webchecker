# Create database

from config import Config
from elasticsearch import helpers
import traceback
import time

config = Config()

def time_backward(t):
    return time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t))

class User:
    def __init__(self):
        pass

    def get(self):
        conn, cur = config.get_mysql()
        cur.execute('SELECT * FROM `users`')
        result = cur.fetchall()
        config.append_mysql(conn, cur)
        return result

class Url:
    def __init__(self):
        pass

    def get(self):
        conn, cur = config.get_mysql()
        cur.execute('SELECT id, url FROM `urls`')
        result = cur.fetchall()
        config.append_mysql(conn, cur)
        return result

    def count(self):
        conn, cur = config.get_mysql()
        cur.execute('SELECT COUNT(*) FROM `urls`')
        result = cur.fetchone()[0]
        config.append_mysql(conn, cur)
        return result

    def get_user_url(self):
        conn, cur = config.get_mysql()
        cur.execute('''SELECT u.id,m.user_id FROM masters AS m, urls AS u, master_urls AS mu WHERE mu.master_id=m.id AND mu.url_id=u.id''')
        result = cur.fetchall()
        config.append_mysql(conn, cur)
        return result

    def update(self, status, url_id, time_response, status_code):
        assert status == 0 or status == 1
        conn, cur = config.get_mysql()
        cur.execute('UPDATE `urls` SET status=%s, time_response=\'%s\', status_code=%s WHERE id = %s' % (
            status,
            time_response,
            status_code, 
            url_id
        ))
        conn.commit()
        config.append_mysql(conn, cur)

    def update_time_response(self, url_id, time_response):
        conn, cur = config.get_mysql()
        cur.execute('UPDATE `urls` SET time_response=\'%s\' WHERE id = %s' % (
            time_response,
            url_id
        ))
        conn.commit()
        config.append_mysql(conn, cur)


    def update_status(self, url_id, status):
        conn, cur = config.get_mysql()
        cur.execute('UPDATE `urls` SET status=%s WHERE id = %s' % (
            status,
            url_id
        ))
        conn.commit()
        config.append_mysql(conn, cur)

class Master:
    def __init__(self):
        pass

    def get(self):
        conn, cur = config.get_mysql()
        cur.execute('SELECT * FROM `masters`')
        result = cur.fetchall()
        config.append_mysql(conn, cur)
        return result

class MasterUrl:
    def __init__(self):
        pass

    def get(self):
        conn, cur = config.get_mysql()
        cur.execute('SELECT * FROM `master_urls`')
        result = cur.fetchall()
        config.append_mysql(conn, cur)
        return result

    def increase_up_time(self, url_id, interval):
        conn, cur = config.get_mysql()
        query = 'UPDATE `master_urls` SET up_time=up_time+%d WHERE url_id=%d' % (int(interval), url_id)
        cur.execute(query)
        conn.commit()
        config.append_mysql(conn, cur)

    def update_created_at(self, url_id, time=time.time()):
        conn, cur = config.get_mysql()
        query = 'UPDATE `master_urls` SET created_at="%s" WHERE url_id=%d AND created_at=NULL' % (time_backward(time), url_id)
        cur.execute(query)
        conn.commit()
        config.append_mysql(conn, cur)

def reload():
    config.reload()        

class Event:
    
    def __init__(self):
        pass

    def insert(self, event_generator):
        es = config.get_es()
        try:
            helpers.bulk(es, event_generator)
        except:
            print(traceback.format_exc())
            time.sleep(1)
        config.append_es(es)

    def update_duration(self, url_id, duration):
        body = {
            'script' : {
                'inline': 'ctx._source.duration = %s' % duration,
                'lang' : 'painless'
            },
            'query' : {
                'bool': {'must': [{'term' : {'url_id': url_id}},{'term' : {'duration': 0}}]}
            }
        }
        es = config.get_es()
        while 1:
            print(time.ctime(time.time())),
            print('url_id:', url_id),
            print('duration:', duration),
            try:
                es.update_by_query(index='webassistant3', doc_type='event', body=body)
            except:
                print(traceback.format_exc())
                time.sleep(1)
            else:
                break

        config.append_es(es)




    def get_start_events(self):
        es = config.get_es()
        query = {'query': {'match': {'type':'Start'}}}
        for item in helpers.scan(es, index='webassistant3', scroll='1m', query=query, request_timeout=120, raise_on_error=False):
            yield {'url_id': item['_source']['url_id'], 'user_id': item['_source']['user_id']}
        config.append_es(es)

    def get_last_datapoint(self, url_id):
        es = config.get_es()
        query = {
            'query': {'match': {'url_id': url_id}},
            'size' : 1,
            'sort' : [{'timestamp' : {'order': 'desc'}}]
        }
        result = es.search(index='webassistant3', doc_type='datapoint', body=query)['hits']['hits']
        if len(result) == 0:
            result = None
        else:
            result = result[0]['_source']
        config.append_es(es)
        return result

if __name__ == '__main__':
    pass