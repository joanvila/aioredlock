import asyncio
import logging

from aioredlock import Aioredlock, LockError


async def lock_context():
    lock_manager = Aioredlock([
        'redis://localhost:6379/0',
        'redis://localhost:6379/1',
        'redis://localhost:6379/2',
        'redis://localhost:6379/3',
    ],
    lock_timeout=1.0
    )

    if await lock_manager.is_locked("resource"):
        print('The resource is already acquired')

    try:
        async with await lock_manager.lock("resource") as lock:
            assert lock.valid is True
            assert await lock_manager.is_locked("resource") is True
            # Do your stuff having the lock
            await asyncio.sleep(lock_manager.lock_timeout * 2)
            # lock manager will extend the lock automatically
            assert await lock_manager.is_locked(lock)
            # or you can extend your lock's lifetime manually
            await lock_manager.extend(lock)
            # Do more stuff having the lock and if you spend much more time than you expected, the lock might be freed
            await asyncio.sleep(lock_manager.lock_timeout * 6)
            assert await lock_manager.is_locked(lock) is False

        assert lock.valid is False  # lock will be released by context manager
    except LockError:
        print('"resource" key might be not empty. Please call '
              '"del resource" in redis-cli')
        raise

    assert lock.valid is False
    assert await lock_manager.is_locked("resource") is False

    await lock_manager.destroy()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(lock_context())
