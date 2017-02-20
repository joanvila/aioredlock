import pytest

from collections import namedtuple

Redis_config = namedtuple('Redis_config', ['host', 'port'])


@pytest.fixture
def single_redis_connection():
    return [{
        'host': 'localhost',
        'port': 6379
    }]
