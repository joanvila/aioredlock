import re
import ssl
import urllib.parse

import aioredis.sentinel


class SentinelConfigError(Exception):
    '''
    Exception raised if Configuration is not valid when instantiating a
    Sentinel object.
    '''


class Sentinel:

    def __init__(self, connection, master=None, password=None, db=None, ssl_context=None):
        '''
        The connection address can be one of the following:
         * a dict - {'host': 'localhost', 'port': 6379}
         * a Redis URI - "redis://host:6379/0?encoding=utf-8&master=mymaster";
         * a (host, port) tuple - ('localhost', 6379);
         * or a unix domain socket path string - "/path/to/redis.sock".
         * a redis connection pool.

        :param connection:
            The connection address can be one of the following:
                * a dict - {
                    'host': 'localhost',
                    'port': 26379,
                    'password': 'insecure',
                    'db': 0,
                    'master': 'mymaster',
                }
                * a Redis URI - "redis://:insecure@host:26379/0?master=mymaster&encoding=utf-8";
                * a (host, port) tuple - ('localhost', 26379);
        :param master: The name of the master to connect to via the sentinel
        :param password: The password to use to connect to the redis master
        :param db: The db to use on the redis master
        :param ssl_context: The ssl context to assign to the redis connection.
            If ssl_context is ``True``, the default ssl context in python will be assigned, otherwise
            an ssl context must be provided.

        Explicitly specified parameters overwrite implicit options in the ``connection`` variable.

        For example, if 'master' is specified in the connection dictionary,
        but also specified as the master kwarg, the master kwarg will be used
        instead.
        '''
        address, kwargs = (), {}
        if isinstance(connection, dict):
            kwargs.update(connection)
            address = [(kwargs.pop('host'), kwargs.pop('port', 26379))]
        elif isinstance(connection, str) and re.match(r'^rediss?://.*\:\d+/\d?\??.*$', connection):
            url = urllib.parse.urlparse(connection)
            query = {key: value[0] for key, value in urllib.parse.parse_qs(url.query).items()}
            address = [(url.hostname, url.port or 6379)]
            dbnum = url.path.strip('/')

            if url.scheme == 'rediss':
                kwargs['ssl'] = ssl.create_default_context()
                verify_mode = query.pop('ssl_cert_reqs', None)
                if verify_mode is not None and hasattr(ssl, verify_mode.upper()):
                    if verify_mode == 'CERT_NONE':
                        kwargs['ssl'].check_hostname = False
                    kwargs['ssl'].verify_mode = getattr(ssl, verify_mode.upper())

            kwargs['db'] = int(dbnum) if dbnum.isdigit() else 0
            kwargs['password'] = url.password
            kwargs.update(query)

        elif isinstance(connection, tuple):
            address = [connection]
        elif isinstance(connection, list):
            address = connection
        else:
            raise SentinelConfigError('Invalid Sentinel Configuration')

        if db is not None:
            kwargs['db'] = db
        if password is not None:
            kwargs['password'] = password
        if ssl_context is True:
            kwargs['ssl'] = ssl.create_default_context()
        elif ssl_context is not None:
            kwargs['ssl'] = ssl_context

        self.master = kwargs.pop('master', None)
        if master:
            self.master = master

        if self.master is None:
            raise SentinelConfigError('Master name required for sentinel to be configured')

        kwargs['minsize'] = 1 if 'minsize' not in kwargs else int(kwargs['minsize'])
        kwargs['maxsize'] = 100 if 'maxsize' not in kwargs else int(kwargs['maxsize'])

        self.connection = address
        self.redis_kwargs = kwargs

    async def get_sentinel(self):
        '''
        Retrieve sentinel object from aioredis.
        '''
        return await aioredis.sentinel.create_sentinel(
            sentinels=self.connection,
            **self.redis_kwargs,
        )

    async def get_master(self):
        '''
        Get ``Redis`` instance for specified ``master``
        '''
        sentinel = await self.get_sentinel()
        return await sentinel.master_for(self.master)
