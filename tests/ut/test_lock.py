from aioredlock import Lock


class TestLock:

    def test_lock(self):
        lock = Lock()
        assert lock.valid is False
