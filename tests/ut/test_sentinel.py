import asyncio
import contextlib
import ssl
import sys
from unittest import mock

import aioredlock.sentinel
from aioredlock.sentinel import Sentinel
from aioredlock.sentinel import SentinelConfigError

import pytest

pytestmark = [pytest.mark.asyncio]


@contextlib.contextmanager
def mock_aioredis_sentinel():
    if sys.version_info < (3, 8, 0):
        mock_obj = mock.MagicMock()
        mock_obj.master_for.return_value = asyncio.Future()
        mock_obj.master_for.return_value.set_result(True)
    else:
        mock_obj = mock.AsyncMock()
        mock_obj.master_for.return_value = True
    with mock.patch.object(aioredlock.sentinel.aioredis.sentinel, 'create_sentinel') as mock_sentinel:
        if sys.version_info < (3, 8, 0):
            mock_sentinel.return_value = asyncio.Future()
            mock_sentinel.return_value.set_result(mock_obj)
        else:
            mock_sentinel.return_value = mock_obj
        yield mock_sentinel


async def test_sentinel_dict():
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel({
            'host': '127.0.0.1',
            'port': 26379,
            'master': 'leader',
        })
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(sentinels=[('127.0.0.1', 26379)], minsize=1, maxsize=100)
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('leader')


async def test_sentinel_str():
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel(
            'redis://:password@localhost:12345/0?master=whatever&encoding=utf-8&minsize=2&maxsize=5'
        )
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(
        sentinels=[('localhost', 12345)],
        db=0,
        encoding='utf-8',
        password='password',
        minsize=2,
        maxsize=5
    )
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('whatever')


async def test_sentinel_str_overrides():
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel(
            'redis://:password@localhost:12345/0?master=whatever&encoding=utf-8',
            master='everything',
            password='newpass',
            db=3,
        )
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(
        sentinels=[('localhost', 12345)],
        db=3,
        encoding='utf-8',
        password='newpass',
        minsize=1,
        maxsize=100
    )
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('everything')


async def test_sentinel_str_ssl_default(ssl_context):
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel('rediss://:password@localhost:12345/2?master=whatever&encoding=utf-8')
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(
        sentinels=[('localhost', 12345)],
        db=2,
        encoding='utf-8',
        password='password',
        minsize=1,
        maxsize=100,
        ssl=ssl_context
    )
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('whatever')
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode is ssl.CERT_REQUIRED


async def test_sentinel_str_ssl():
    ssl_context = ssl.create_default_context()
    with mock_aioredis_sentinel() as mock_sentinel, \
            mock.patch('ssl.create_default_context', return_value=ssl_context):
        sentinel = Sentinel(
            'rediss://:password@localhost:12345/2?'
            'master=whatever&encoding=utf-8&ssl_cert_reqs=CERT_NONE',
        )
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(
        sentinels=[('localhost', 12345)],
        db=2,
        encoding='utf-8',
        password='password',
        minsize=1,
        maxsize=100,
        ssl=ssl_context
    )
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('whatever')
    assert ssl_context.check_hostname is False
    assert ssl_context.verify_mode is ssl.CERT_NONE


async def test_sentinel_str_ssl_cert_optional():
    ssl_context = ssl.create_default_context()
    with mock_aioredis_sentinel() as mock_sentinel, \
            mock.patch('ssl.create_default_context', return_value=ssl_context):
        sentinel = Sentinel(
            'rediss://:password@localhost:12345/2?'
            'master=whatever&encoding=utf-8&ssl_cert_reqs=CERT_OPTIONAL',
        )
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(
        sentinels=[('localhost', 12345)],
        db=2,
        encoding='utf-8',
        password='password',
        minsize=1,
        maxsize=100,
        ssl=ssl_context
    )
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('whatever')
    assert ssl_context.check_hostname is True
    assert ssl_context.verify_mode is ssl.CERT_OPTIONAL


async def test_sentinel_tuple():
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel(
            ('127.0.0.1', 1234),
            master='blah',
            ssl_context=False,
        )
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(sentinels=[('127.0.0.1', 1234)], ssl=False, minsize=1, maxsize=100)
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('blah')


async def test_sentinel_list():
    with mock_aioredis_sentinel() as mock_sentinel:
        sentinel = Sentinel(
            [('127.0.0.1', 1234), ('blah', 4829)],
            master='blah',
        )
        assert await sentinel.get_master()
    assert mock_sentinel.called
    mock_sentinel.assert_called_with(sentinels=[('127.0.0.1', 1234), ('blah', 4829)], minsize=1, maxsize=100)
    if sys.version_info < (3, 8, 0):
        result = mock_sentinel.return_value.result()
    else:
        result = mock_sentinel.return_value
    assert result.master_for.called
    result.master_for.assert_called_with('blah')


async def test_sentinel_no_master_specified():
    with pytest.raises(SentinelConfigError):
        Sentinel('redis://localhost:1234/0')


async def test_sentinel_bad_database():
    with pytest.raises(SentinelConfigError):
        Sentinel('redis://localhost:1234/blah')


async def test_sentinel_invalid_connection():
    with pytest.raises(SentinelConfigError):
        Sentinel(object())
