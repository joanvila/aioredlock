import asyncio
from aioredlock import Aioredlock, LockError


async def basic_lock():
    lock_manager = Aioredlock([{
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'password': None
    }])

    try:
        lock = await lock_manager.lock("resource")
    except LockError:
        print('"resource" key might be not empty. Please call '
              '"del resource" in redis-cli')
        raise
    assert lock.valid is True

    # Do your stuff having the lock

    await lock_manager.unlock(lock)
    assert lock.valid is False

    await lock_manager.destroy()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(basic_lock())
