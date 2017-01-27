from aioredlock import Aioredlock


class TestLock:

    def test_default_initialization(self):
        lock_manager = Aioredlock()
        assert lock_manager.redis_host == 'localhost'
        assert lock_manager.redis_port == 6379
