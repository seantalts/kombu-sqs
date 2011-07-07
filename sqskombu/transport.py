from Queue import Empty
from kombu.transport import virtual

from boto.sqs.message import Message
from boto.exception import SQSError
from anyjson import serialize, deserialize

import socket
import time

class Channel(virtual.Channel):
    _instance = None

    def __init__(self, *args,  **kwargs):
        super(Channel, self).__init__(*args, **kwargs)
        self.conninfo = self.connection.client
        cache_class = self.conninfo.transport_options["cache_class"]
        self.cache = cache_class and cache_class()
        if not self.cache:
            self.dedupe = lambda x, y: False
        self.connclass = self.conninfo.transport_options["connection_class"]
        self.throttle = self.conninfo.transport_options["throttle"]

    def dedupe(self, message):
        """woopdedupe

        warning: deletes seen messages"""
        #message.id is not the same as the celery id
        str_id = "sqsdedupe:"+str(message.id)
        if str_id in self.cache:
            message.delete()
            return True
        self.cache[str_id] = 1
        return False

    @staticmethod
    def normalize_queue_name(queue):
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
        if m and not self.dedupe(m):
            msg = deserialize(m.get_body())
            q.delete_message(m)
            return msg
        else:
            if getattr(self, '_last_get', None):
                time_passed = time.time() - self._last_get
                time_to_sleep = self.throttle - time_passed
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
        return self.connclass(self.conninfo.userid, self.conninfo.password)

    @property
    def client(self):
        if not hasattr(self, '_client'):
            self._client = self._open()
            self._queues = dict()
        return self._client

    def close(self):
        self.exchange_types = {} #remove circular references
        super(Channel, self).close()

class SQSTransport(virtual.Transport):

    Channel = Channel

    connection_errors = (socket.error,)
    channel_errors = (SQSError,)
