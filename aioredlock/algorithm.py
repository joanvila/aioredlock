from aioredlock.lock import Lock


class Aioredlock:

    def __init__(self, host='localhost', port=6379):
        self.redis_host = host
        self.redis_port = port

    async def lock(self, resource, timeout=10):
        """
        Tries to acquire de lock.
        If the lock is correctly acquired, the valid property of the lock is true.

        :return: :class:`aioredlock.Lock`
        """
        valid = True
        return Lock(valid=valid)
