import pytest

from collections import namedtuple

Redis_config = namedtuple('Redis_config', ['host', 'port'])


@pytest.fixture
def redis_connection():
    return Redis_config(
        host='localhost',
        port=6379
    )
