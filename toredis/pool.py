from collections import deque
import logging

from toredis.errors import TooManyClients
from toredis.client import Client


logger = logging.getLogger(__name__)


class ClientPool(object):
    '''Redis Client Pool
    '''
    @property
    def stat_info(self):
        return {'pool_size': self._pool_size,
                'actives': len(self._active_cache),
                'idles': len(self._idle_cache)}

    def __init__(self,
                 pool_size=8,
                 host='127.0.0.1',
                 port=6379,
                 reuse_actives=True,
                 *args, **kwargs):
        self._pool_size = pool_size
        self._host = host
        self._port = port
        self._reuse_actives = reuse_actives
        self._args, self._kwargs = args, kwargs

        self._idle_cache = deque()
        self._active_cache = deque()

    def _new_client(self):
        if self._pool_size and len(self._active_cache) >= self._pool_size:
            if self._reuse_actives:
                return self._active_cache[0]

            raise TooManyClients("TOO MANY CLIENTS: %d, %d" %
                    (len(self._active_cache), self._pool_size))

        logger.info('MAKE NEW CLIENT')

        kwargs = self._kwargs
        kwargs['pool'] = self

        _client = Client(*self._args, **kwargs)
        _client.connect(host=self._host, port=self._port)

        return _client

    def _active(self, client):
        if client in self._active_cache:
            return

        self._active_cache.append(client)

    @property
    def client(self):
        try:
            _client = self._idle_cache.popleft()

            if not _client.is_connected():
                _client.connect(host=self._host,
                port=self._port)
        except IndexError as e:
            _client = self._new_client()

        self._active(_client)

        return _client

    def _de_active(self, client):
        try:
            self._active_cache.remove(client)
        except ValueError as e:
            logger.error('ACTIVE CLIENT IS NOT FOUND')

    def cache(self, client):
        self._de_active(client)

        if client in self._idle_cache:
            return

        self._idle_cache.append(client)

    def close(self):
        def _close(_cache):
            while _cache:
                client = _cache.popleft()
                try:
                    client.close()
                except Exception as e:
                    logging.error(e)

        _close(self._idle_cache)
        _close(self._active_cache)
