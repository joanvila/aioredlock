from aioredlock import Aioredlock, Lock


class TestLock:

    def test_lock(self):
        lock_manager = Aioredlock()
        lock = Lock(lock_manager, "potato", 1)
        assert lock.resource == "potato"
        assert lock.id == 1
        assert lock.valid is False
