from aioredlock import Lock


class TestLock:

    def test_lock(self):
        lock = Lock("potato")
        assert lock.resource == "potato"
        assert lock.valid is False
