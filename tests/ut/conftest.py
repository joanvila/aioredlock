import pytest
from asynctest import CoroutineMock

from aioredlock import Aioredlock


class FakePool:

    SET_IF_NOT_EXIST = 'SET_IF_NOT_EXIST'

    def __init__(self):
        self.set = CoroutineMock(return_value=True)
        self.eval = CoroutineMock()

    def __await__(self):
        yield
        return self

    def __enter__(self):
        return self

    def __exit__(self, *args, **kwargs):
        pass

    def __call__(self):
        return self


@pytest.fixture
def lock_manager():
    lock_manager = Aioredlock()
    pool = FakePool()
    lock_manager._connect = pool
    lock_manager.LOCK_TIMEOUT = 1
    yield lock_manager, pool
