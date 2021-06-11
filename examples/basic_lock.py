import asyncio
import logging

from aioredlock import Aioredlock, LockError, LockAcquiringError


async def basic_lock():
    lock_manager = Aioredlock([{
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'password': None
    }])

    if await lock_manager.is_locked("resource"):
        print('The resource is already acquired')

    try:
        lock = await lock_manager.lock("resource")
    except LockAcquiringError:
        print('Something happened during normal operation. We just log it.')
    except LockError:
        print('Something is really wrong and we prefer to raise the exception')
        raise
    assert lock.valid is True
    assert await lock_manager.is_locked("resource") is True

    # Do your stuff having the lock

    await lock_manager.unlock(lock)
    assert lock.valid is False
    assert await lock_manager.is_locked("resource") is False

    await lock_manager.destroy()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(basic_lock())
