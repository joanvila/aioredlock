import pytest

from aioredlock import Aioredlock, Lock


class TestLock:

    def test_lock(self):
        lock_manager = Aioredlock()
        lock = Lock(lock_manager, "potato", 1)
        assert lock.resource == "potato"
        assert lock.id == 1
        assert lock.valid is False

    @pytest.mark.asyncio
    async def test_extend(self, aioredlock_patched):
        lock = await aioredlock_patched.lock('foo')
        await lock.extend()
        aioredlock_patched.extend.assert_called_once_with(lock)

    @pytest.mark.asyncio
    async def test_release(self, aioredlock_patched):
        lock = await aioredlock_patched.lock('foo')
        await lock.release()
        aioredlock_patched.unlock.assert_called_once_with(lock)
