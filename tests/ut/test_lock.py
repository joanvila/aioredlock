from aioredlock import Lock


class TestLock:

    def test_lock(self):
        lock = Lock("potato", 1)
        assert lock.resource == "potato"
        assert lock.id == 1
        assert lock.valid is False
