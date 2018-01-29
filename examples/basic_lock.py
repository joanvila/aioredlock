import asyncio
from aioredlock import Aioredlock


async def basic_lock():
    lock_manager = Aioredlock([{
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'password': None
    }])

    lock = await lock_manager.lock("resource")
    assert lock.valid is True

    # Do your stuff having the lock

    await lock_manager.unlock(lock)
    assert lock.valid is False

    await lock_manager.destroy()


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(basic_lock())
