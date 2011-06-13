from Queue import Empty
from kombu.transport import virtual

from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message
from anyjson import serialize, deserialize

import socket
import time

try:
    import pylibmc
except ImportError:
    pylibmc = None

#TODO how to make this configurable?
THROTTLE = 30 #only poll every 30 seconds

class Channel(virtual.Channel):
    def __init__(self, *args,  **kwargs):
        virtual.Channel.__init__(self, *args, **kwargs)
        self.conninfo = self.connection.client
        if pylibmc:
            memcached_servers = self.conninfo.transport_options["memcached_servers"]
            self.cache = pylibmc.Client(memcached_servers)
            self.cache_set = lambda x: self.cache.set("celery:"+x, 1, time=3600) # 1hour
            print "self.cache:", self.cache
        else:
            print "Warning:  pylibmc not found!  Memcache will not be used for SQS deduplication."
            self.cache = {}
            self.cache_set = lambda x: self.cache.set(x, 1)

    def seen(self, message):
        """warning: deletes seen messages"""
        str_id = str(message.id) #message.id is not the same as the celery id
        if self.cache.get(str_id):
            message.delete()
            return True
        self.cache_set(str_id)
        return False

    def normalize_queue_name(self, queue):
        """
        A queue name must conform to the following::

            Can only include alphanumeric characters, hyphens, or underscores. 1 to 80 in length

        This function aims to map a non-standard name to one that is acceptable for sqs
        """
        return queue.replace('.', '_')

    def get_or_create_queue(self, queue):
        self.client #initial client if we don't have it
        name = self.normalize_queue_name(queue)
        if name not in self._queues:
            self._queues[name] = self.client.create_queue(name)
        return self._queues[name]

    def _new_queue(self, queue, **kwargs):
        self.get_or_create_queue(queue)

    def _put(self, queue, message, **kwargs):
        q = self.get_or_create_queue(queue)
        m = Message()
        m.set_body(serialize(message))
        assert q.write(m)

    def _get(self, queue):
        q = self.get_or_create_queue(queue)
        m = q.read()
        if m and not self.seen(m):
            msg = deserialize(m.get_body())
            q.delete_message(m)
            return msg
        else:
            if getattr(self, '_last_get', None):
                time_passed = time.time() - self._last_get
                time_to_sleep = THROTTLE - time_passed
                if time_to_sleep > 0:
                    time.sleep(time_to_sleep)
            self._last_get = time.time()
        raise Empty()

    def _size(self, queue):
        q = self.get_or_create_queue(queue)
        return q.count()

    def _purge(self, queue):
        q = self.get_or_create_queue(queue)
        count = q.count()
        q.clear()
        return count #CONSIDER this number may not be accurate

    def _open(self):
        return SQSConnection(self.conninfo.userid, self.conninfo.password)

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = self._open()
            self._queues = dict()
        return self._client

class SQSTransport(virtual.Transport):
    Channel = Channel

    connection_errors = (socket.error)
    channel_errors = ()

