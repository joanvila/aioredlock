import asyncio
import logging

from aioredlock import Aioredlock, LockError


async def lock_context():
    lock_manager = Aioredlock([{
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'password': None
    }])

    try:
        async with await lock_manager.lock("resource") as lock:
            assert lock.valid is True
            # Do your stuff having the lock
            await lock.extend()
            # Do more stuff having the lock
        assert lock.valid is False  # lock will be released by context manager
    except LockError:
        print('"resource" key might be not empty. Please call '
              '"del resource" in redis-cli')
        raise

    await lock_manager.destroy()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(lock_context())
